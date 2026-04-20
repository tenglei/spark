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

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for [[LiquidCacheExtension]], [[LiquidCacheColumnarRule]],
 * and [[LiquidCacheBatchScanExec]].
 *
 * These tests verify the extension registration and plan rewriting behavior
 * without requiring a running Liquid Cache server. The plugin's Flight client
 * is a stub that always returns cache-miss, so queries fall back to the
 * original data source transparently.
 */
class LiquidCacheExtensionSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", classOf[LiquidCacheExtension].getName)
  }

  test("extension registers successfully") {
    // Verify the SparkSession was created without errors when the extension is loaded
    assert(spark.sessionState != null)
  }

  test("cache disabled by default - no interception") {
    withTempPath { dir =>
      spark.range(100).write.parquet(dir.getAbsolutePath)
      val df = spark.read.parquet(dir.getAbsolutePath)
      val plan = df.queryExecution.executedPlan

      // When cache is disabled (default), BatchScanExec should remain unwrapped
      val batchScans = plan.collect { case b: BatchScanExec => b }
      assert(batchScans.nonEmpty, "Should use BatchScanExec when cache is disabled")

      val liquidScans = plan.collect { case l: LiquidCacheBatchScanExec => l }
      assert(liquidScans.isEmpty,
        "Should not use LiquidCacheBatchScanExec when cache is disabled")
    }
  }

  test("cache enabled - BatchScanExec is wrapped with LiquidCacheBatchScanExec") {
    withTempPath { dir =>
      spark.range(100).write.parquet(dir.getAbsolutePath)
      withSQLConf(
        "liquidcache.enabled" -> "true",
        "liquidcache.server.address" -> "localhost:15214"
      ) {
        val df = spark.read.parquet(dir.getAbsolutePath)
        val plan = df.queryExecution.executedPlan

        // When cache is enabled, BatchScanExec should be replaced by LiquidCacheBatchScanExec
        val liquidScans = plan.collect { case l: LiquidCacheBatchScanExec => l }
        assert(liquidScans.nonEmpty,
          "Should use LiquidCacheBatchScanExec when cache is enabled")

        // The original BatchScanExec should no longer appear at the top level
        val topLevelBatchScans = plan.collect {
          case b: BatchScanExec => b
        }
        assert(topLevelBatchScans.isEmpty,
          "Top-level BatchScanExec should be replaced by LiquidCacheBatchScanExec")
      }
    }
  }

  test("cache enabled - query executes correctly with stub client (cache-miss fallback)") {
    withTempPath { dir =>
      val expected = spark.range(100)
      expected.write.parquet(dir.getAbsolutePath)

      withSQLConf(
        "liquidcache.enabled" -> "true",
        "liquidcache.server.address" -> "localhost:15214"
      ) {
        val df = spark.read.parquet(dir.getAbsolutePath)
        // The stub client always returns cache-miss, so data is read from the original source.
        // Verify the data is still correct.
        checkAnswer(df, expected.toDF())
      }
    }
  }

  test("LiquidCacheBatchScanExec preserves output schema") {
    withTempPath { dir =>
      spark.range(50).selectExpr("id", "id * 2 as doubled").write.parquet(dir.getAbsolutePath)

      withSQLConf(
        "liquidcache.enabled" -> "true",
        "liquidcache.server.address" -> "localhost:15214"
      ) {
        val df = spark.read.parquet(dir.getAbsolutePath)
        val plan = df.queryExecution.executedPlan
        val liquidScans = plan.collect { case l: LiquidCacheBatchScanExec => l }
        assert(liquidScans.nonEmpty)

        val scanOutput = liquidScans.head.output
        assert(scanOutput.map(_.name) === Seq("id", "doubled"),
          "LiquidCacheBatchScanExec should preserve the output schema of the original scan")
      }
    }
  }

  test("only file-based scans are intercepted") {
    // Non-file-based scans (e.g., in-memory) should not be wrapped
    withSQLConf(
      "liquidcache.enabled" -> "true",
      "liquidcache.server.address" -> "localhost:15214"
    ) {
      val df = spark.range(10).toDF()
      val plan = df.queryExecution.executedPlan
      val liquidScans = plan.collect { case l: LiquidCacheBatchScanExec => l }
      assert(liquidScans.isEmpty,
        "Non-file-based scans should not be wrapped by LiquidCacheBatchScanExec")
    }
  }

  test("node name appears in explain output") {
    withTempPath { dir =>
      spark.range(10).write.parquet(dir.getAbsolutePath)
      withSQLConf(
        "liquidcache.enabled" -> "true",
        "liquidcache.server.address" -> "localhost:15214"
      ) {
        val df = spark.read.parquet(dir.getAbsolutePath)
        val explainStr = df.queryExecution.executedPlan.toString()
        assert(explainStr.contains("LiquidCacheBatchScan"),
          s"Explain output should contain 'LiquidCacheBatchScan', got:\n$explainStr")
      }
    }
  }
}
