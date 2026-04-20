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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging

/**
 * JNI-based client for Liquid Cache server.
 * Maintains one session per server address per JVM (executor singleton).
 *
 * The server transparently caches data - repeated reads of the same
 * Parquet files are served from cache automatically.
 */
object LiquidCacheJniClient extends Logging {
  private val sessions = new ConcurrentHashMap[String, Long]()
  private val registeredStores = new ConcurrentHashMap[String, Boolean]()

  /**
   * Ensures the given object store URL is registered with the Liquid Cache server.
   * Registration is deduplicated per (serverAddress, storeUrl) pair so each bucket
   * is only registered once per JVM (executor).
   */
  def ensureObjectStoreRegistered(
      serverAddress: String,
      storeUrl: String,
      options: Map[String, String]): Unit = {
    val key = s"$serverAddress:$storeUrl"
    registeredStores.computeIfAbsent(key, _ => {
      try {
        registerObjectStore(serverAddress, storeUrl, options)
        logInfo(s"Registered object store: $storeUrl on $serverAddress")
        true
      } catch {
        case e: Exception =>
          logWarning(s"Failed to register object store $storeUrl: ${e.getMessage}", e)
          true // Mark as attempted to avoid repeated failures
      }
    })
  }

  def getOrCreateSession(serverAddress: String): Long = {
    sessions.computeIfAbsent(serverAddress, addr => {
      LiquidCacheNative.ensureLoaded()
      val session = LiquidCacheNative.createSession(addr)
      logInfo(s"Created Liquid Cache session to $addr (handle=$session)")
      session
    })
  }

  def registerObjectStore(
      serverAddress: String, url: String, options: Map[String, String]): Unit = {
    val session = getOrCreateSession(serverAddress)
    val optionsArray = options.map { case (k, v) => s"$k=$v" }.toArray
    LiquidCacheNative.registerObjectStore(session, url, optionsArray)
  }

  def registerParquet(serverAddress: String, tableName: String, filePath: String): Unit = {
    val session = getOrCreateSession(serverAddress)
    LiquidCacheNative.registerParquet(session, tableName, filePath)
  }

  def executeScan(
      serverAddress: String,
      tableName: String,
      columns: Array[String],
      batchSize: Int): Long = {
    val session = getOrCreateSession(serverAddress)
    LiquidCacheNative.executeScan(session, tableName, columns, batchSize)
  }

  def fetchNextBatch(resultHandle: Long): Array[Byte] = {
    LiquidCacheNative.fetchNextBatch(resultHandle)
  }

  def closeResult(resultHandle: Long): Unit = {
    LiquidCacheNative.closeResult(resultHandle)
  }

  def shutdown(): Unit = {
    sessions.forEach { (_, handle) =>
      try { LiquidCacheNative.closeSession(handle) }
      catch { case _: Exception => }
    }
    sessions.clear()
    registeredStores.clear()
  }
}
