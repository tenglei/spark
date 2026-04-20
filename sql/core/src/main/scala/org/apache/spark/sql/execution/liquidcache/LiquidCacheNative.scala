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

/**
 * JNI bridge to the liquid-cache-jni native library.
 * Provides access to Liquid Cache server via the native Rust client.
 *
 * The native library handles:
 * - Arrow Flight connection to Liquid Cache server
 * - DataFusion physical plan construction for Parquet scans
 * - Plan serialization and execution delegation
 * - Arrow IPC result streaming
 */
object LiquidCacheNative {
  // Load native library - expects libliquid_cache_jni.so / liquid_cache_jni.dll on java.library.path
  private var loaded = false

  def ensureLoaded(): Unit = synchronized {
    if (!loaded) {
      System.loadLibrary("liquid_cache_jni")
      loaded = true
    }
  }

  @native def createSession(serverAddress: String): Long
  @native def registerObjectStore(session: Long, url: String, options: Array[String]): Unit
  @native def registerParquet(session: Long, tableName: String, filePath: String): Unit
  @native def executeScan(
      session: Long, tableName: String, columns: Array[String], batchSize: Int): Long
  @native def fetchNextBatch(result: Long): Array[Byte] // Arrow IPC bytes, null when done
  @native def closeResult(result: Long): Unit
  @native def closeSession(session: Long): Unit
}
