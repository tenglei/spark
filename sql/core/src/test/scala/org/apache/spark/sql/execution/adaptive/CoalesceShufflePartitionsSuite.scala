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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec}
import org.apache.spark.sql.internal.SQLConf

class CoalesceShufflePartitionsSuite extends SparkFunSuite {

  private def createMapOutputStats(
      bytesByPartition: Array[Long],
      recordsByPartition: Array[Long]): MapOutputStatistics = {
    MapOutputStatistics(bytesByPartition, recordsByPartition)
  }

  test("coalesce partitions by rows - basic case") {
    val stats = Seq(
      Some(createMapOutputStats(
        Array(100L, 200L, 300L, 400L),
        Array(10L, 20L, 30L, 40L))))

    val specs = Seq(Some(Seq(
      CoalescedPartitionSpec(0, 1, Seq(0)),
      CoalescedPartitionSpec(1, 2, Seq(1)),
      CoalescedPartitionSpec(2, 3, Seq(2)),
      CoalescedPartitionSpec(3, 4, Seq(3)))))

    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      stats, specs, targetRows = 25, minNumPartitions = 1, minPartitionRows = 1)

    assert(result.size == 2)
    assert(result.head.size == 2)
    assert(result.last.size == 2)
  }

  test("coalesce partitions by rows - respect min partition rows") {
    val stats = Seq(
      Some(createMapOutputStats(
        Array(100L, 200L, 300L, 400L),
        Array(10L, 20L, 30L, 40L))))

    val specs = Seq(Some(Seq(
      CoalescedPartitionSpec(0, 1, Seq(0)),
      CoalescedPartitionSpec(1, 2, Seq(1)),
      CoalescedPartitionSpec(2, 3, Seq(2)),
      CoalescedPartitionSpec(3, 4, Seq(3)))))

    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      stats, specs, targetRows = 25, minNumPartitions = 1, minPartitionRows = 30)

    assert(result.size == 3)
    assert(result.head.size == 1)
    assert(result(1).size == 2)
    assert(result.last.size == 1)
  }

  test("coalesce partitions by rows - respect min num partitions") {
    val stats = Seq(
      Some(createMapOutputStats(
        Array(100L, 200L, 300L, 400L),
        Array(10L, 20L, 30L, 40L))))

    val specs = Seq(Some(Seq(
      CoalescedPartitionSpec(0, 1, Seq(0)),
      CoalescedPartitionSpec(1, 2, Seq(1)),
      CoalescedPartitionSpec(2, 3, Seq(2)),
      CoalescedPartitionSpec(3, 4, Seq(3)))))

    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      stats, specs, targetRows = 100, minNumPartitions = 3, minPartitionRows = 1)

    assert(result.size == 3)
    assert(result.head.size == 1)
    assert(result(1).size == 2)
    assert(result.last.size == 1)
  }

  test("coalesce partitions by rows - empty partitions") {
    val stats = Seq(
      Some(createMapOutputStats(
        Array(0L, 0L, 0L, 0L),
        Array(0L, 0L, 0L, 0L))))

    val specs = Seq(Some(Seq(
      CoalescedPartitionSpec(0, 1, Seq(0)),
      CoalescedPartitionSpec(1, 2, Seq(1)),
      CoalescedPartitionSpec(2, 3, Seq(2)),
      CoalescedPartitionSpec(3, 4, Seq(3)))))

    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      stats, specs, targetRows = 100, minNumPartitions = 1, minPartitionRows = 1)

    assert(result.size == 1)
    assert(result.head.size == 4)
  }

  test("coalesce partitions by rows - single large partition") {
    val stats = Seq(
      Some(createMapOutputStats(
        Array(1000L),
        Array(100L))))

    val specs = Seq(Some(Seq(
      CoalescedPartitionSpec(0, 1, Seq(0)))))

    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      stats, specs, targetRows = 50, minNumPartitions = 1, minPartitionRows = 1)

    assert(result.size == 2)
    assert(result.head.size == 1)
    assert(result.last.size == 1)
  }
}
