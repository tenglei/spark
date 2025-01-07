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
import org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil.CoalescedPartitionSpec

class CoalesceShufflePartitionsSuite extends SparkFunSuite {

  // Basic Scenarios
  test("uniform distribution - basic") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(1000L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(1000L, 1000L, 1000L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 2),
      CoalescedPartitionSpec(2, 4)
    ))
  }

  test("single skewed partition - left") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(5000L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(5000L, 1000L, 1000L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  test("multiple skewed partitions - middle and right") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(1000L, 5000L, 1000L, 3000L)),
      new MapOutputStatistics(1, Array(1000L, 5000L, 1000L, 3000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 2),
      CoalescedPartitionSpec(2, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  test("all small partitions below min threshold") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(50L, 50L, 50L, 50L)),
      new MapOutputStatistics(1, Array(50L, 50L, 50L, 50L))
    )
    val targetRows = 200L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 4)
    ))
  }

  test("all large partitions above target") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(5000L, 5000L, 5000L, 5000L)),
      new MapOutputStatistics(1, Array(5000L, 5000L, 5000L, 5000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 2),
      CoalescedPartitionSpec(2, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  // Boundary Conditions
  test("empty first partition") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(0L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(0L, 1000L, 1000L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 2),
      CoalescedPartitionSpec(2, 4)
    ))
  }

  test("empty last partition") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(1000L, 1000L, 1000L, 0L)),
      new MapOutputStatistics(1, Array(1000L, 1000L, 1000L, 0L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 2),
      CoalescedPartitionSpec(2, 4)
    ))
  }

  test("single non-empty partition") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(0L, 0L, 0L, 1000L)),
      new MapOutputStatistics(1, Array(0L, 0L, 0L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 4)
    ))
  }

  // Complex Combinations
  test("skew + small + empty partitions") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(5000L, 50L, 0L, 1000L, 50L, 3000L)),
      new MapOutputStatistics(1, Array(5000L, 50L, 0L, 1000L, 50L, 3000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 6, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 2),
      CoalescedPartitionSpec(2, 3),
      CoalescedPartitionSpec(3, 4),
      CoalescedPartitionSpec(4, 5),
      CoalescedPartitionSpec(5, 6)
    ))
  }

  // Parameter Variations
  test("different target row sizes") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(1000L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(1000L, 1000L, 1000L, 1000L))
    )
    val targetRows = 500L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 2),
      CoalescedPartitionSpec(2, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  test("extreme min partition row threshold") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(1000L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(1000L, 1000L, 1000L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 5000L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 4)
    ))
  }

  test("multiple shuffle stages with different skew patterns") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(5000L, 1000L, 1000L, 1000L)), // left skew
      new MapOutputStatistics(1, Array(1000L, 1000L, 5000L, 1000L)), // middle skew
      new MapOutputStatistics(2, Array(1000L, 1000L, 1000L, 5000L))  // right skew
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  test("extremely large partitions needing split") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(10000L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(10000L, 1000L, 1000L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 4)
    ))
  }

  test("minPartitionRows affecting coalescing") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(500L, 500L, 500L, 500L)),
      new MapOutputStatistics(1, Array(500L, 500L, 500L, 500L))
    )
    val targetRows = 1000L
    val minPartitionRows = 600L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 2),
      CoalescedPartitionSpec(2, 4)
    ))
  }

  test("mixed empty and skewed partitions") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(0L, 5000L, 0L, 1000L)),
      new MapOutputStatistics(1, Array(0L, 5000L, 0L, 1000L))
    )
    val targetRows = 2000L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 2),
      CoalescedPartitionSpec(2, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  test("very small target row size") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(100L, 100L, 100L, 100L)),
      new MapOutputStatistics(1, Array(100L, 100L, 100L, 100L))
    )
    val targetRows = 50L
    val minPartitionRows = 10L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 1),
      CoalescedPartitionSpec(1, 2),
      CoalescedPartitionSpec(2, 3),
      CoalescedPartitionSpec(3, 4)
    ))
  }

  test("uneven distribution across shuffle stages") {
    val mapStats = Seq(
      new MapOutputStatistics(0, Array(1000L, 1000L, 1000L, 1000L)),
      new MapOutputStatistics(1, Array(2000L, 2000L, 2000L, 2000L)),
      new MapOutputStatistics(2, Array(500L, 500L, 500L, 500L))
    )
    val targetRows = 1500L
    val minPartitionRows = 100L
    
    val result = ShufflePartitionsUtil.coalescePartitionsByRows(
      0, 4, mapStats, targetRows, minPartitionRows)
      
    assert(result === Seq(
      CoalescedPartitionSpec(0, 2),
      CoalescedPartitionSpec(2, 4)
    ))
  }
}
