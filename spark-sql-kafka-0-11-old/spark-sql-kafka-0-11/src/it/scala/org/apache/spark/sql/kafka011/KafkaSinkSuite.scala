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

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.time.SpanSugar._
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BinaryType, DataType}


class KafkaSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  protected var testUtils: KafkaTestUtils = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(
      withBrokerProps = Map("auto.create.topics.enable" -> "false"))
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("batch - write to kafka") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    val df = Seq("1", "2", "3", "4", "5").map(v => (topic, v)).toDF("topic", "value")
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", topic)
      .save()
    checkAnswer(
      createKafkaReader(topic).selectExpr("CAST(value as STRING) value"),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }
  
  test("batch - write to kafka with headers") {
    val topic = newTopic()
    testUtils.createTopic(topic)

    val df = Seq("1", "2", "3", "4", "5")
      .map(v => (topic, v))
      .toDF("topic", "value")
      .withColumn("headers",
                  array(struct(lit("key").as("headerKey"), lit("value".getBytes).as("headerValue")),
                        struct(lit("key2").as("headerKey"), lit("value2".getBytes).as("headerValue"))))

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", topic)
      .save()

    val messagesFromKafka = createKafkaReader(topic)
      .selectExpr("CAST(value as STRING) value", "explode(headers) as header")
      .selectExpr("value", "header.headerKey", "CAST(header.headerValue as STRING) headerValue")

    checkAnswer(
      messagesFromKafka,
      Row("1", "key", "value") ::
        Row("1",  "key2", "value2") ::
        Row("2",  "key", "value") ::
        Row("2",  "key2", "value2") ::
        Row("3",  "key", "value") ::
        Row("3",  "key2", "value2") ::
        Row("4",  "key", "value") ::
        Row("4",  "key2", "value2") ::
        Row("5",  "key", "value") ::
        Row("5",  "key2", "value2") :: Nil)
   }

  test("batch - null topic field value, and no topic option") {
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")
    val ex = intercept[SparkException] {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "null topic present in the data"))
  }

  test("batch - unsupported save modes") {
    val topic = newTopic()
    testUtils.createTopic(topic)
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")

    // Test bad save mode Ignore
    var ex = intercept[AnalysisException] {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .mode(SaveMode.Ignore)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      s"save mode ignore not allowed for kafka"))

    // Test bad save mode Overwrite
    ex = intercept[AnalysisException] {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      s"save mode overwrite not allowed for kafka"))
  }

  test("SPARK-20496: batch - enforce analyzed plans") {
    val inputEvents =
      spark.range(1, 1000)
        .select(to_json(struct("*")) as 'value)

    val topic = newTopic()
    testUtils.createTopic(topic)
    // used to throw UnresolvedException
    inputEvents.write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", topic)
      .save()
  }

  test("streaming - write to kafka with topic field") {
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF(),
      withTopic = None,
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as topic", "value")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
      .as[(Int, Int)]
      .map(_._2)

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }
  
  test("streaming - write to kafka with headers") {
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    val dfWithHeaders = input
      .toDF()
      .withColumn("headers",
                  array(struct(lit("key").as("headerKey"), lit("value".getBytes).as("headerValue")),
                        struct(lit("key2").as("headerKey"), lit("value2".getBytes).as("headerValue"))))
    
    val writer = createKafkaWriter(
      dfWithHeaders,
      withTopic = None,
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as topic", "value", "headers")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(value as STRING) value", "explode(headers) as header")
      .selectExpr("CAST(value as INT) value", "header.headerKey", "CAST(header.headerValue as STRING) headerValue")
      .as[(Int, String, String)]

    try {
      val data = 1 to 10
      val expectedData = data.flatMap(n => Seq((n, "key", "value"), (n, "key2", "value2"))).toList
      val firstBatchData = data.slice(0, 5).map(_.toString)
      val firstBatchExpected = expectedData.slice(0, 10)
      input.addData(firstBatchData)
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, firstBatchExpected: _*)
      val secondBatchData = data.slice(5, 10).map(_.toString)
      val secondBatchExpected = expectedData
      input.addData(secondBatchData)
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, secondBatchExpected: _*)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write aggregation w/o topic field, with topic option") {
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF().groupBy("value").count(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr = "CAST(value as STRING) key", "CAST(count as STRING) value")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
      .as[(Int, Int)]

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3))
      input.addData("1", "2", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4))
    } finally {
      writer.stop()
    }
  }

  test("streaming - aggregation with topic field and topic option") {
    /* The purpose of this test is to ensure that the topic option
     * overrides the topic field. We begin by writing some data that
     * includes a topic field and value (e.g., 'foo') along with a topic
     * option. Then when we read from the topic specified in the option
     * we should see the data i.e., the data was written to the topic
     * option, and not to the topic in the data e.g., foo
     */
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF().groupBy("value").count(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr = "'foo' as topic",
        "CAST(value as STRING) key", "CAST(count as STRING) value")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key AS INT)", "CAST(value AS INT)")
      .as[(Int, Int)]

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3))
      input.addData("1", "2", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4))
    } finally {
      writer.stop()
    }
  }


  test("streaming - write data with bad schema") {
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    /* No topic field or topic option */
    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = "value as key", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("topic option required when no 'topic' attribute is present"))

    try {
      /* No value field */
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"'$topic' as topic", "value as key"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "required attribute 'value' not found"))
  }

  test("streaming - write data with valid schema but wrong types") {
    val input = MemoryStream[String]
    val topic = newTopic()
    testUtils.createTopic(topic)

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      /* topic field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"CAST('1' as INT) as topic", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("topic type must be a stringtype"))

    try {
      ex = intercept[StreamingQueryException] {
        /* key field wrong type */
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as key", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("key attribute type must be a stringtype or binarytype"))
    
    try {
      /* value field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("value attribute type must be a stringtype or binarytype"))
    
    try {
      /* header field wrong type */
      ex = intercept[StreamingQueryException] {
        val inputWithHeaders = input
          .toDF()
          .withColumn("headers", array(struct(lit("1").as("headerKey").cast("INT"), // should be string
                                              lit("1".getBytes).as("headerValue"))))
        writer = createKafkaWriter(inputWithHeaders)(
          withSelectExpr = s"'$topic' as topic", "value", "headers"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
     } finally {
       writer.stop()
     }
     assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
       "headers attribute type must be an arraytype of non-null elements of type structtype with a non-null field " +
       "named headerkey of type stringtype and a field named headervalue of type binarytype"))
  }

  test("streaming - write to non-existing topic") {
    val input = MemoryStream[String]
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF(), withTopic = Some(topic))()
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("job aborted"))
  }

  test("streaming - exception on config serializer") {
    val input = MemoryStream[String]
    var writer: StreamingQuery = null
    var ex: Exception = null
    ex = intercept[IllegalArgumentException] {
      writer = createKafkaWriter(
        input.toDF(),
        withOptions = Map("kafka.key.serializer" -> "foo"))()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "kafka option 'key.serializer' is not supported"))

    ex = intercept[IllegalArgumentException] {
      writer = createKafkaWriter(
        input.toDF(),
        withOptions = Map("kafka.value.serializer" -> "foo"))()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "kafka option 'value.serializer' is not supported"))
  }

  test("generic - write big data with small producer buffer") {
    /* This test ensures that we understand the semantics of Kafka when
    * is comes to blocking on a call to send when the send buffer is full.
    * This test will configure the smallest possible producer buffer and
    * indicate that we should block when it is full. Thus, no exception should
    * be thrown in the case of a full buffer.
    */
    val topic = newTopic()
    testUtils.createTopic(topic, 1)
    val options = new java.util.HashMap[String, Object]
    options.put("bootstrap.servers", testUtils.brokerAddress)
    options.put("buffer.memory", "16384") // min buffer size
    // this emulates the pre-0.11 "block.on.buffer.full" configuration by setting the longest max blocking time possible
    options.put("max.block.ms", Long.MaxValue.toString)
    options.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    options.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask = new KafkaWriteTask(options, inputSchema, Some(topic))
    try {
      val fieldTypes: Array[DataType] = Array(BinaryType)
      val converter = UnsafeProjection.create(fieldTypes)
      val row = new SpecificInternalRow(fieldTypes)
      row.update(0, data)
      val iter = Seq.fill(1000)(converter.apply(row)).iterator
      writeTask.execute(iter)
    } finally {
      writeTask.close()
    }
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def createKafkaReader(topic: String): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("subscribe", topic)
      .load()
  }

  private def createKafkaWriter(
      input: DataFrame,
      withTopic: Option[String] = None,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.length > 0) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .queryName("kafkaStream")
      withTopic.foreach(stream.option("topic", _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }
}
