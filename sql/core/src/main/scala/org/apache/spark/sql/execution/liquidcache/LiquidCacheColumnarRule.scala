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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan

/**
 * A [[ColumnarRule]] that intercepts [[BatchScanExec]] nodes whose underlying scan
 * is file-based (Parquet or ORC) and replaces them with [[LiquidCacheBatchScanExec]].
 *
 * The replacement only occurs during `preColumnarTransitions` so that Spark's automatic
 * columnar-to-row / row-to-columnar transitions are correctly inserted afterwards.
 *
 * @param session the active [[SparkSession]] used to read configuration properties
 */
class LiquidCacheColumnarRule(session: SparkSession) extends ColumnarRule with Logging {

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    val enabled = session.conf.get("liquidcache.enabled", "false").toBoolean
    if (!enabled) {
      plan
    } else {
      val serverAddress = session.conf.get(
        "liquidcache.server.address", "localhost:15214")
      logInfo(s"LiquidCache enabled, server=$serverAddress. " +
        "Scanning plan for eligible BatchScanExec nodes.")
      plan.transformUp {
        case batch: BatchScanExec if isFileBasedScan(batch.scan) =>
          logInfo(s"Replacing BatchScanExec with LiquidCacheBatchScanExec " +
            s"for scan: ${batch.scan.description()}")
          LiquidCacheBatchScanExec(batch, serverAddress)
      }
    }
  }

  /**
   * Returns `true` if the given [[Scan]] is a file-based scan that we can cache,
   * currently Parquet and ORC.
   */
  private def isFileBasedScan(scan: Scan): Boolean = {
    scan.isInstanceOf[ParquetScan] || scan.isInstanceOf[OrcScan]
  }
}
