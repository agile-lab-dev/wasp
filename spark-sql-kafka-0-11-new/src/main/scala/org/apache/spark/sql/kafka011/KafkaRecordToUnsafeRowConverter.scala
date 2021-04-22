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

import it.agilelab.bigdata.wasp.spark.sql.kafka011.KafkaSparkSQLSchemas
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp
import scala.collection.JavaConverters._

/** A simple class for converting Kafka ConsumerRecord to UnsafeRow */
private[kafka011] class KafkaRecordToUnsafeRowConverter {
  private val toUnsafeRowWithHeaders = UnsafeProjection.create(KafkaSparkSQLSchemas.INPUT_SCHEMA)

  def toUnsafeRow: ConsumerRecord[Array[Byte], Array[Byte]] => UnsafeRow =
    (cr: ConsumerRecord[Array[Byte], Array[Byte]]) => toUnsafeRowWithHeaders(toInternalRow(cr))

  def toInternalRow(cr:  ConsumerRecord[Array[Byte], Array[Byte]]): InternalRow = {
    InternalRow(
      cr.key,
      cr.value,
      if (cr.headers.iterator().hasNext) {
        new GenericArrayData(cr.headers.iterator().asScala
          .map(header =>
            InternalRow(UTF8String.fromString(header.key()), header.value())
          ).toArray)
      } else {
        null
      },
      UTF8String.fromString(cr.topic),
      cr.partition,
      cr.offset,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(cr.timestamp)),
      cr.timestampType.id
    )
  }
}
