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

import java.security.MessageDigest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * A [[PartitionReaderFactory]] wrapper that creates JNI-based partition readers.
 *
 * For columnar reads, it produces [[LiquidCachePartitionReader]] instances that delegate
 * file reads to the Liquid Cache server via JNI. The server handles caching transparently.
 *
 * For row-based reads, it delegates directly to the original factory since the cache
 * operates on columnar batches for efficiency.
 *
 * @param originalFactory      the original [[PartitionReaderFactory]] from the data source
 * @param serverAddress        the Liquid Cache server address (host:port)
 * @param schemaCatalogString  the catalog string of the read schema, used as part of the cache key
 * @param columns              the column names to project in the scan
 * @param hadoopConf           serializable Hadoop configuration for S3 credential extraction
 */
class LiquidCachePartitionReaderFactory(
    originalFactory: PartitionReaderFactory,
    serverAddress: String,
    schemaCatalogString: String,
    columns: Array[String],
    hadoopConf: SerializableConfiguration
) extends PartitionReaderFactory with Logging with Serializable {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    // Row-based reads bypass the cache and delegate directly to the original factory.
    originalFactory.createReader(partition)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val cacheKey = computeCacheKey(partition)
    logDebug(s"Creating LiquidCachePartitionReader with cacheKey=$cacheKey " +
      s"for partition=$partition")
    new LiquidCachePartitionReader(
      cacheKey,
      partition,
      serverAddress,
      columns,
      hadoopConf,
      Some(originalFactory)
    )
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    originalFactory.supportColumnarReads(partition)
  }

  /**
   * Computes a deterministic cache key for the given partition.
   *
   * For [[FilePartition]], the key is derived from sorted file paths, offsets, lengths,
   * and the schema hash to ensure uniqueness across different column projections.
   *
   * For other partition types, a fallback MD5 of `toString` + schema hash is used.
   */
  private[liquidcache] def computeCacheKey(partition: InputPartition): String = {
    val raw = partition match {
      case fp: FilePartition =>
        val fileInfo = fp.files.map { f: PartitionedFile =>
          s"${f.filePath}:${f.start}:${f.length}"
        }.sorted.mkString("|")
        s"$fileInfo:${schemaCatalogString.hashCode}"
      case other =>
        s"${other.toString}:${schemaCatalogString.hashCode}"
    }
    md5Hex(raw)
  }

  /** Computes MD5 hex digest without depending on commons-codec. */
  private def md5Hex(input: String): String = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(input.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }
}
