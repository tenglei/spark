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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

case class CoalescedPartitionSpec(
  start: Int,
  end: Int,
  startIndices: Seq[Int] = Seq.empty) extends ShufflePartitionSpec {
  
  override def toString: String = {
    if (startIndices.isEmpty) {
      s"CoalescedPartitionSpec($start, $end)"
    } else {
      s"CoalescedPartitionSpec($start, $end, $startIndices)"
    }
  }
}

object ShufflePartitionsUtil {

  def estimateAverageRowSize(mapOutputStats: Seq[Option[MapOutputStatistics]]): Double = {
    val validStats = mapOutputStats.flatten
    if (validStats.isEmpty) {
      return 0.0
    }

    val totalRows = validStats.map(_.recordsByPartitionId.sum).sum
    val totalSize = validStats.map(_.bytesByPartitionId.sum).sum

    if (totalRows == 0) 0.0 else totalSize.toDouble / totalRows
  }

  def coalescePartitionsByRows(
      mapOutputStats: Seq[Option[MapOutputStatistics]],
      partitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]],
      targetRows: Long,
      minNumPartitions: Int,
      minPartitionRows: Long): Seq[Seq[ShufflePartitionSpec]] = {
    val validStats = mapOutputStats.flatten
    if (validStats.isEmpty) {
      return Seq.empty
    }

    val numPartitions = validStats.head.bytesByPartitionId.length
    val newPartitionSpecs = mutable.ArrayBuffer.empty[Seq[ShufflePartitionSpec]]

    var currentPartitionStart = 0
    var currentPartitionRows = 0L
    var currentPartitionSize = 0L

    for (i <- 0 until numPartitions) {
      val partitionRows = validStats.map(_.recordsByPartitionId(i)).sum
      val partitionSize = validStats.map(_.bytesByPartitionId(i)).sum

      if (currentPartitionRows + partitionRows > targetRows &&
          currentPartitionRows >= minPartitionRows) {
        newPartitionSpecs.append(createPartitionSpecs(
          currentPartitionStart, i, partitionSpecs))
        currentPartitionStart = i
        currentPartitionRows = 0L
        currentPartitionSize = 0L
      }

      currentPartitionRows = currentPartitionRows + partitionRows
      currentPartitionSize = currentPartitionSize + partitionSize
    }

    if (currentPartitionRows > 0) {
      newPartitionSpecs += createPartitionSpecs(
        currentPartitionStart, numPartitions, partitionSpecs)
    }

    if (newPartitionSpecs.size < minNumPartitions) {
      adjustPartitionSpecs(newPartitionSpecs, minNumPartitions)
    } else {
      newPartitionSpecs.toSeq
    }
  }

  private def createPartitionSpecs(
      start: Int,
      end: Int,
      partitionSpecs: Seq[Option[Seq[ShufflePartitionSpec]]]): Seq[ShufflePartitionSpec] = {
    partitionSpecs.map { specs =>
      specs.map { s =>
        CoalescedPartitionSpec(start, end, s.map(_.startIndices).getOrElse(Seq.empty))
      }.getOrElse(Seq(CoalescedPartitionSpec(start, end, Seq.empty)))
    }.flatten
  }

  private def adjustPartitionSpecs(
      partitionSpecs: mutable.ArrayBuffer[Seq[ShufflePartitionSpec]],
      minNumPartitions: Int): Seq[Seq[ShufflePartitionSpec]] = {
    while (partitionSpecs.size < minNumPartitions) {
      val largestPartitionIndex = partitionSpecs.zipWithIndex.maxBy(_._1.size)._2
      val largestPartition = partitionSpecs(largestPartitionIndex)
      val (firstHalf, secondHalf) = splitPartitionSpec(largestPartition)
      partitionSpecs(largestPartitionIndex) = firstHalf
      partitionSpecs.insert(largestPartitionIndex + 1, secondHalf)
    }
    partitionSpecs.toSeq
  }

  private def splitPartitionSpec(
      specs: Seq[ShufflePartitionSpec]): (Seq[ShufflePartitionSpec], Seq[ShufflePartitionSpec]) = {
    val midPoint = specs.size / 2
    (specs.take(midPoint), specs.drop(midPoint))
  }
}

case class MapOutputStatistics(
  bytesByPartitionId: Array[Long],
  recordsByPartitionId: Array[Long]) {

  def totalSize: Long = bytesByPartitionId.sum
  def totalRows: Long = recordsByPartitionId.sum
}
