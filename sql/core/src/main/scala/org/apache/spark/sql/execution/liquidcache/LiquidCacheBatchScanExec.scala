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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceRDD}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * A wrapper physical operator that replaces [[BatchScanExec]] to provide transparent
 * Liquid Cache integration. This node delegates all structural properties (output schema,
 * partitioning, ordering) to the original [[BatchScanExec]], but wraps the partition reader
 * factory with [[LiquidCachePartitionReaderFactory]] to enable cache-aside reads.
 *
 * == Design ==
 * This is a [[LeafExecNode]] that mirrors the behavior of the original [[BatchScanExec]]:
 *  - `output`, `outputPartitioning`, `outputOrdering` are all delegated.
 *  - `supportsColumnar` follows the original scan's capabilities.
 *  - `inputRDD` creates a [[DataSourceRDD]] with a wrapped reader factory that checks
 *    the Liquid Cache server before falling back to the original data source.
 *
 * == Cache Key Generation ==
 * For [[FilePartition]]-based partitions, the cache key is derived from:
 * `MD5(filePath:start:length|... + readSchema hashCode)`
 *
 * For other partition types, a fallback based on `toString` is used.
 *
 * @param originalBatchScan the original [[BatchScanExec]] being wrapped
 * @param serverAddress     the Arrow Flight server address (host:port)
 */
case class LiquidCacheBatchScanExec(
    originalBatchScan: BatchScanExec,
    serverAddress: String) extends LeafExecNode {

  // ---------------------------------------------------------------------------
  // Delegated structural properties
  // ---------------------------------------------------------------------------

  override def output: Seq[Attribute] = originalBatchScan.output

  override def outputPartitioning: Partitioning = originalBatchScan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = originalBatchScan.outputOrdering

  override def supportsColumnar: Boolean = originalBatchScan.supportsColumnar

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "cacheHits" -> SQLMetrics.createMetric(sparkContext, "number of cache hits"),
    "cacheMisses" -> SQLMetrics.createMetric(sparkContext, "number of cache misses")
  ) ++ originalBatchScan.scan.supportedCustomMetrics().map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  // ---------------------------------------------------------------------------
  // Schema info for cache key computation
  // ---------------------------------------------------------------------------

  private val schemaCatalogString: String = originalBatchScan.scan.readSchema().catalogString

  private val columns: Array[String] = originalBatchScan.scan.readSchema().fieldNames

  // ---------------------------------------------------------------------------
  // Reader factory & RDD construction
  // ---------------------------------------------------------------------------

  @transient private lazy val batch = originalBatchScan.batch

  @transient private lazy val originalReaderFactory = batch.createReaderFactory()

  @transient private lazy val hadoopConf = new SerializableConfiguration(
    sparkSession.sessionState.newHadoopConf())

  private lazy val wrappedReaderFactory = new LiquidCachePartitionReaderFactory(
    originalReaderFactory,
    serverAddress,
    schemaCatalogString,
    columns,
    hadoopConf
  )

  /**
   * Build the partitions list using the same logic as [[BatchScanExec]].
   * We reuse the original batch's `planInputPartitions` directly.
   */
  @transient private lazy val inputPartitions: Seq[Seq[InputPartition]] =
    originalBatchScan.partitions

  @transient lazy val inputRDD: RDD[InternalRow] = {
    val customMetrics = originalBatchScan.scan.supportedCustomMetrics().map { customMetric =>
      customMetric.name() -> metrics(customMetric.name())
    }.toMap

    new DataSourceRDD(
      sparkContext,
      inputPartitions,
      wrappedReaderFactory,
      supportsColumnar,
      customMetrics
    )
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }

  // ---------------------------------------------------------------------------
  // Display
  // ---------------------------------------------------------------------------

  override def simpleString(maxFields: Int): String = {
    val scanDesc = originalBatchScan.scan.description()
    s"LiquidCacheBatchScan $scanDesc server=$serverAddress"
  }

  override def nodeName: String = "LiquidCacheBatchScan"
}
