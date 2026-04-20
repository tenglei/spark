/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.liquidcache

import java.io.ByteArrayInputStream
import java.net.URI

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.apache.spark.util.SerializableConfiguration

/**
 * A columnar [[PartitionReader]] that delegates file reads to the Liquid Cache server via JNI.
 *
 * == Read Path ==
 * On first access (lazy initialization):
 *  1. Extract the file path from the [[FilePartition]].
 *  2. Register the Parquet file with the Liquid Cache server.
 *  3. Execute a scan with the requested column projection.
 *  4. Iterate over Arrow IPC batches returned from the native layer.
 *  5. Convert each Arrow IPC batch to a Spark [[ColumnarBatch]].
 *
 * The Liquid Cache server transparently caches data - repeated reads of the same
 * Parquet files are served from cache automatically.
 *
 * == Fallback ==
 * If the JNI layer fails (native library not loaded, server unavailable), the reader
 * falls back to the original [[PartitionReader]] from the wrapped factory.
 *
 * @param cacheKey        the unique cache key for this partition's data
 * @param partition       the original [[InputPartition]]
 * @param serverAddress   the Liquid Cache server address (host:port)
 * @param columns         the column names to project in the scan
 * @param hadoopConf      serializable Hadoop configuration for extracting S3/OBS credentials
 * @param fallbackFactory optional fallback factory for graceful degradation
 */
class LiquidCachePartitionReader(
    cacheKey: String,
    partition: InputPartition,
    serverAddress: String,
    columns: Array[String],
    hadoopConf: SerializableConfiguration,
    fallbackFactory: Option[org.apache.spark.sql.connector.read.PartitionReaderFactory] = None
) extends PartitionReader[ColumnarBatch] with Logging {

  private var resultHandle: Long = -1
  private var currentBatch: ColumnarBatch = _
  private var initialized = false
  private var finished = false
  private var usingFallback = false
  private var fallbackReader: PartitionReader[ColumnarBatch] = _

  /**
   * Normalizes Hadoop S3 scheme variants (s3a://, s3n://) and OBS scheme to s3://
   * as expected by the Liquid Cache server's object_store crate.
   * OBS (Huawei Object Storage Service) is S3-compatible and accessed via S3 protocol.
   */
  private def normalizeFilePath(filePath: String): String = {
    if (filePath.startsWith("s3a://")) {
      "s3://" + filePath.stripPrefix("s3a://")
    } else if (filePath.startsWith("s3n://")) {
      "s3://" + filePath.stripPrefix("s3n://")
    } else if (filePath.startsWith("obs://")) {
      "s3://" + filePath.stripPrefix("obs://")  // OBS is S3-compatible
    } else {
      filePath
    }
  }

  /**
   * Detects S3/OBS paths and registers the object store with the Liquid Cache server,
   * extracting credentials from the Hadoop configuration.
   * Supports bucket-specific and general configuration keys for both S3A and OBS.
   */
  private def ensureObjectStoreRegistered(filePath: String): Unit = {
    val uri = new URI(filePath)
    val scheme = if (uri.getScheme != null) uri.getScheme.toLowerCase else null

    scheme match {
      case "s3" | "s3a" | "s3n" =>
        val bucket = uri.getHost
        val storeUrl = s"s3://$bucket"
        val options = extractS3Options(bucket)
        LiquidCacheJniClient.ensureObjectStoreRegistered(
          serverAddress, storeUrl, options)

      case "obs" =>
        // Huawei OBS is S3-compatible; register as S3 with OBS credentials and endpoint
        val bucket = uri.getHost
        val storeUrl = s"s3://$bucket"
        val options = extractObsOptions(bucket)
        LiquidCacheJniClient.ensureObjectStoreRegistered(
          serverAddress, storeUrl, options)

      case _ => // Local filesystem (no scheme, or file://), no registration needed
    }
  }

  /**
   * Extracts S3 credentials and configuration from Hadoop config.
   * Supports bucket-specific keys (fs.s3a.bucket.BUCKET.*) with fallback to general keys.
   */
  private def extractS3Options(bucket: String): Map[String, String] = {
    val conf = hadoopConf.value
    val options = Map.newBuilder[String, String]

    // Access key: try bucket-specific first, then general
    val accessKey = Option(conf.get(s"fs.s3a.bucket.$bucket.access.key"))
      .orElse(Option(conf.get("fs.s3a.access.key")))
    accessKey.foreach(k => options += ("aws_access_key_id" -> k))

    // Secret key
    val secretKey = Option(conf.get(s"fs.s3a.bucket.$bucket.secret.key"))
      .orElse(Option(conf.get("fs.s3a.secret.key")))
    secretKey.foreach(k => options += ("aws_secret_access_key" -> k))

    // Region
    val region = Option(conf.get(s"fs.s3a.bucket.$bucket.endpoint.region"))
      .orElse(Option(conf.get("fs.s3a.endpoint.region")))
      .orElse(Option(conf.get("fs.s3a.region")))
    region.foreach(r => options += ("region" -> r))

    // Endpoint (for MinIO or custom S3-compatible services)
    val endpoint = Option(conf.get(s"fs.s3a.bucket.$bucket.endpoint"))
      .orElse(Option(conf.get("fs.s3a.endpoint")))
    endpoint.foreach(e => options += ("endpoint" -> e))

    // Path style access (common for MinIO)
    val pathStyle = Option(conf.get("fs.s3a.path.style.access"))
    pathStyle.foreach(p => options += ("aws_allow_http" -> p))

    options.result()
  }

  /**
   * Extracts Huawei OBS credentials and configuration from Hadoop config.
   * OBS is S3-compatible, so credentials are mapped to AWS-style options.
   * Supports bucket-specific keys (fs.obs.bucket.BUCKET.*) with fallback to general keys.
   */
  private def extractObsOptions(bucket: String): Map[String, String] = {
    val conf = hadoopConf.value
    val options = Map.newBuilder[String, String]

    // OBS access key
    val accessKey = Option(conf.get(s"fs.obs.bucket.$bucket.access.key"))
      .orElse(Option(conf.get("fs.obs.access.key")))
    accessKey.foreach(k => options += ("aws_access_key_id" -> k))

    // OBS secret key
    val secretKey = Option(conf.get(s"fs.obs.bucket.$bucket.secret.key"))
      .orElse(Option(conf.get("fs.obs.secret.key")))
    secretKey.foreach(k => options += ("aws_secret_access_key" -> k))

    // OBS endpoint
    val endpoint = Option(conf.get(s"fs.obs.bucket.$bucket.endpoint"))
      .orElse(Option(conf.get("fs.obs.endpoint")))
    endpoint.foreach(e => options += ("endpoint" -> e))

    // OBS region: explicit config or extract from endpoint URL
    val region = Option(conf.get("fs.obs.region"))
      .orElse(endpoint.flatMap(extractRegionFromObsEndpoint))
    region.foreach(r => options += ("region" -> r))

    // Force path-style access — OBS requires this; without it the client would
    // attempt virtual-hosted style (bucket.obs.region.myhuaweicloud.com) which may not resolve
    options += ("aws_virtual_hosted_style_request" -> "false")

    // Allow HTTP if endpoint is not HTTPS
    endpoint.foreach { e =>
      if (e.startsWith("http://")) {
        options += ("allow_http" -> "true")
      }
    }

    options.result()
  }

  /**
   * Extracts region from a Huawei OBS endpoint URL.
   * Example: "https://obs.cn-north-4.myhuaweicloud.com" -> Some("cn-north-4")
   */
  private def extractRegionFromObsEndpoint(endpoint: String): Option[String] = {
    val pattern = """obs\.([^.]+)\.myhuaweicloud\.com""".r
    pattern.findFirstMatchIn(endpoint).map(_.group(1))
  }

  private def initialize(): Unit = {
    if (!initialized) {
      initialized = true
      try {
        partition match {
          case fp: FilePartition =>
            val filePath = fp.files.head.filePath.toString
            ensureObjectStoreRegistered(filePath)
            val normalizedPath = normalizeFilePath(filePath)
            resultHandle = LiquidCacheJniClient.executeScan(
              serverAddress, normalizedPath, columns, 8192)
          case _ =>
            logWarning(s"Unsupported partition type: ${partition.getClass}, " +
              s"attempting fallback")
            initFallback()
        }
      } catch {
        case e: UnsatisfiedLinkError =>
          logWarning(s"Native library not available, falling back to original reader " +
            s"for key=$cacheKey", e)
          initFallback()
        case e: Exception =>
          logError(s"Failed to initialize Liquid Cache scan for key=$cacheKey: " +
            s"${e.getMessage}", e)
          initFallback()
      }
    }
  }

  private def initFallback(): Unit = {
    fallbackFactory match {
      case Some(factory) =>
        logInfo(s"Falling back to original reader for key=$cacheKey")
        usingFallback = true
        fallbackReader = factory.createColumnarReader(partition)
      case None =>
        logWarning(s"No fallback factory available for key=$cacheKey, marking finished")
        finished = true
    }
  }

  override def next(): Boolean = {
    initialize()
    if (usingFallback) {
      return fallbackReader.next()
    }
    if (finished) return false

    try {
      val ipcBytes = LiquidCacheJniClient.fetchNextBatch(resultHandle)
      if (ipcBytes == null) {
        finished = true
        false
      } else {
        if (currentBatch != null) {
          currentBatch.close()
        }
        currentBatch = deserializeArrowIpc(ipcBytes)
        true
      }
    } catch {
      case e: Exception =>
        logError(s"Error fetching batch from Liquid Cache for key=$cacheKey: " +
          s"${e.getMessage}", e)
        throw new org.apache.spark.SparkException(
          s"Failed to fetch batch from Liquid Cache server: ${e.getMessage}", e)
    }
  }

  override def get(): ColumnarBatch = {
    if (usingFallback) {
      fallbackReader.get()
    } else {
      currentBatch
    }
  }

  override def close(): Unit = {
    if (usingFallback && fallbackReader != null) {
      try { fallbackReader.close() }
      catch { case e: Exception => logWarning("Error closing fallback reader", e) }
    }
    if (resultHandle >= 0) {
      try { LiquidCacheJniClient.closeResult(resultHandle) }
      catch { case _: Exception => }
    }
    if (currentBatch != null) {
      currentBatch.close()
      currentBatch = null
    }
  }

  /**
   * Deserializes Arrow IPC stream bytes into a Spark [[ColumnarBatch]].
   *
   * Uses the Arrow Java SDK to read the IPC format, then wraps each Arrow FieldVector
   * as a Spark [[ArrowColumnVector]].
   */
  private def deserializeArrowIpc(ipcBytes: Array[Byte]): ColumnarBatch = {
    val allocator = new RootAllocator(Long.MaxValue)
    val stream = new ByteArrayInputStream(ipcBytes)
    val reader = new ArrowStreamReader(stream, allocator)
    try {
      if (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val numRows = root.getRowCount
        val vectors = (0 until root.getFieldVectors.size()).map { i =>
          new ArrowColumnVector(root.getFieldVectors.get(i))
        }
        new ColumnarBatch(vectors.toArray, numRows)
      } else {
        new ColumnarBatch(Array.empty, 0)
      }
    } finally {
      reader.close()
      allocator.close()
    }
  }
}
