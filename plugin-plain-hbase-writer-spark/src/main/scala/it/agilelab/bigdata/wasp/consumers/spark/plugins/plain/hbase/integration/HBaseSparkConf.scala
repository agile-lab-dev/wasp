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

package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import org.apache.hadoop.classification.InterfaceAudience

/**
 * This is the hbase configuration. User can either set them in SparkConf, which
 * will take effect globally, or configure it per table, which will overwrite the value
 * set in SparkConf. If not set, the default value will take effect.
 */
@InterfaceAudience.Public
object HBaseSparkConf{
  /** Set to false to disable server-side caching of blocks for this scan,
   *  false by default, since full table scans generate too much BC churn.
   */
  /** Set to specify the location of hbase configuration file. */
  val HBASE_CONFIG_LOCATION = "hbase.spark.config.location"
  /** Set to specify whether create or use latest cached HBaseContext*/
  val USE_HBASECONTEXT = "hbase.spark.use.hbasecontext"
  val DEFAULT_USE_HBASECONTEXT = false
  /** Pushdown the filter to data source engine to increase the performance of queries. */
  /** The timestamp used to filter columns with a specific timestamp. */
  val TIMESTAMP = "hbase.spark.query.timestamp"
  /** The starting timestamp used to filter columns with a specific range of versions. */
  val TIMERANGE_START = "hbase.spark.query.timerange.start"
  /** The ending timestamp used to filter columns with a specific range of versions. */
  val TIMERANGE_END =  "hbase.spark.query.timerange.end"
  /** The maximum number of version to return. */
  val MAX_VERSIONS = "hbase.spark.query.maxVersions"
  /** Delayed time to close hbase-spark connection when no reference to this connection, in milliseconds. */
  val DEFAULT_CONNECTION_CLOSE_DELAY: Long = 10 * 60 * 1000
}
