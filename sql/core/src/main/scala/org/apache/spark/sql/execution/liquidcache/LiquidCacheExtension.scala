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

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Spark SQL extension that enables transparent Liquid Cache integration.
 *
 * This extension intercepts file-based scan operators (Parquet/ORC) and routes data reads
 * through a Liquid Cache server via Arrow Flight protocol. When enabled, it transparently
 * provides a cache-aside pattern: cache hits are served from the Liquid Cache server,
 * while cache misses fall back to the original data source and asynchronously populate the cache.
 *
 * == Registration ==
 * Register via Spark configuration:
 * {{{
 *   spark.sql.extensions=org.apache.spark.sql.execution.liquidcache.LiquidCacheExtension
 * }}}
 *
 * == Configuration ==
 * The following Spark SQL configuration properties control the behavior:
 *  - `liquidcache.enabled` (default: `false`): Master switch to enable/disable caching.
 *  - `liquidcache.server.address` (default: `localhost:15214`): Arrow Flight server address.
 *
 * == Example ==
 * {{{
 *   val spark = SparkSession.builder()
 *     .config("spark.sql.extensions",
 *       "org.apache.spark.sql.execution.liquidcache.LiquidCacheExtension")
 *     .config("liquidcache.enabled", "true")
 *     .config("liquidcache.server.address", "localhost:15214")
 *     .getOrCreate()
 *
 *   // All subsequent Parquet/ORC reads will be transparently cached
 *   spark.read.parquet("/data/warehouse/table").show()
 * }}}
 */
class LiquidCacheExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(session => new LiquidCacheColumnarRule(session))
  }
}
