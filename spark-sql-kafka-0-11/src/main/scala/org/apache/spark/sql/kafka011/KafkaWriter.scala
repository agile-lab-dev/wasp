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

package org.apache.spark.sql.kafka011

import java.{util => ju}

import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * The [[KafkaWriter]] class is used to write data from a batch query
 * or structured streaming query, given by a [[QueryExecution]], to Kafka.
 * The data is assumed to have a value column, and an optional topic and key
 * columns. If the topic column is missing, then the topic must come from
 * the 'topic' configuration option. If the key column is missing, then a
 * null valued key field will be added to the
 * [[org.apache.kafka.clients.producer.ProducerRecord]].
 */
private[kafka011] object KafkaWriter extends Logging {

  override def toString: String = "KafkaWriter"

  def validateQuery(
      queryExecution: QueryExecution,
      kafkaParameters: ju.Map[String, Object],
      topic: Option[String] = None): Unit = {
    val schema = queryExecution.analyzed.output
    schema.find(_.name == TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (topic.isEmpty) {
        throw new AnalysisException(s"topic option required when no " +
          s"'$TOPIC_ATTRIBUTE_NAME' attribute is present. Use the " +
          s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a topic.")
      } else {
        Literal(topic.get, StringType)
      }
    ).dataType match {
      case StringType => // good
      case _ =>
        throw new AnalysisException(s"Topic type must be a String")
    }
    schema.find(_.name == KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, StringType)
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"$KEY_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    schema.find(_.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new AnalysisException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"$VALUE_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    schema.find(_.name == HEADERS_ATTRIBUTE_NAME).getOrElse(
      Literal(new GenericArrayData(Array.empty[Row]), HEADER_DATA_TYPE_NULL_VALUE)
    ).dataType match {
      case HEADER_DATA_TYPE_NULL_VALUE | HEADER_DATA_TYPE_NON_NULL_VALUE => // good
      case _ =>
        throw new AnalysisException(s"$HEADERS_ATTRIBUTE_NAME attribute type must be an ArrayType of non-null elements" +
          s" of type StructType with a field named $HEADER_KEY_ATTRIBUTE_NAME of type StringType key and a field" +
          s" named $HEADER_VALUE_ATTRIBUTE_NAME of type BinaryType")
    }
  }

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      kafkaParameters: ju.Map[String, Object],
      topic: Option[String] = None): Unit = {
    val schema = queryExecution.analyzed.output
    validateQuery(queryExecution, kafkaParameters, topic)
    val rdd = queryExecution.toRdd
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      rdd.foreachPartition { iter =>
        val writeTask = new KafkaWriteTask(kafkaParameters, schema, topic)
        Utils.tryWithSafeFinally(block = writeTask.execute(iter))(finallyBlock = writeTask.close())
      }
    }
  }
}
