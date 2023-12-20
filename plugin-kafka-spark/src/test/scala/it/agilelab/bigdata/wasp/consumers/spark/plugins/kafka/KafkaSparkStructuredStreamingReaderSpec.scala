package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.TopicModelUtils.topicNameToColumnName
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.configuration._
import it.agilelab.bigdata.wasp.models.{TopicCompression, TopicDataTypes, TopicModel}
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.bson.BsonDocument
import org.scalatest.WordSpec

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

class KafkaSparkStructuredStreamingReaderSpec extends WordSpec with SparkSuite {
  import spark.implicits._
  "single schema mode" should {
    "parse only data in correct column" in {
      val schema = BsonDocument.parse(AvroSchema[MockRecord].toString(true))
      val topic1 = TopicModel(
        "topic1.topic",
        0L,
        0,
        0,
        "json",
        None,
        None,
        Some(Nil),
        false,
        schema,
        TopicCompression.Snappy
      )
      val topic2 = topic1.copy(name = "topic2")
      val topic3 = topic1.copy(name = "topic3")
      val topic4 = topic1.copy(name = "topic4")
      val df = spark
        .createDataset(
          Seq(
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = """{"key":"1", "raw":"valore"}""".getBytes(StandardCharsets.UTF_8),
              headers = Array.empty,
              topic = topic1.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            ),
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = """{"key":"1", "raw":"valore"}""".getBytes(StandardCharsets.UTF_8),
              headers = Array.empty,
              topic = topic2.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            )
          )
        )
        .toDF()

      assert(TopicModelUtils.areTopicsEqualForReading(Seq(topic1, topic2, topic3, topic4)).isRight)

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForOneSchema(topic1, df, Strict)
      assert(outDF.where(col("value").isNull.or(col("key").isNull)).count() === 0)
    }

    "parse correctly in STRICT mode a dataset to json" in {
      val topic = topicTest("json")
      val allRightData = mockedSingleJsonData match {
        case start :+ last =>
          start :+ last.copy(raw = """{"key":"1", "value":"value3"}""".getBytes(StandardCharsets.UTF_8))
      }
      val df = spark.createDataFrame(allRightData)
      val outDf = KafkaSparkStructuredStreamingReader
        .selectForOneSchema(topic, df, Strict)
      assert(outDf.where(col("value").isNull).count() === 0)
    }

    "throw an exception parsing in STRICT mode a dataset with errors to json" in {
      val topic = topicTest("json")
      val df    = spark.createDataFrame(mockedSingleJsonData)

      val outDf = KafkaSparkStructuredStreamingReader
        .selectForOneSchema(topic, df, Strict)

      strictExceptionCheck(outDf, "json")
    }

    "parse correctly in IGNORE mode a dataset with errors to json" in {
      val topic = topicTest("json")
      val df    = spark.createDataFrame(mockedSingleJsonData)

      val outDf = KafkaSparkStructuredStreamingReader.selectForOneSchema(topic, df, Ignore).cache
      assert(outDf.where(col("value").isNull).count() === 0)
      assert(outDf.where(col("value").isNotNull).count() === 2)
      checkResultSchema(outDf, false)
    }

    "parse correctly in HANDLE mode a dataset with errors to json" in {
      val topic = topicTest("json")
      val df    = spark.createDataFrame(mockedSingleJsonData)

      val outDf = KafkaSparkStructuredStreamingReader.selectForOneSchema(topic, df, Handle).cache
      assert(outDf.where(col("raw").isNull).count() === 2)
      assert(outDf.where(col("raw").isNotNull).count() === 1)
      assert(outDf.where(col("value").isNull).count() === 1)
      assert(outDf.where(col("value").isNotNull).count() === 2)
      checkResultSchema(outDf, true)
    }

    "parse correctly in STRICT mode a dataset to avro" in {
      val topic = topicTest("avro")
      val correctedData = mockedSingleAvroData match {
        case start :+ last =>
          start :+ last.copy(raw = {
            val bos = new ByteArrayOutputStream()
            val aos = AvroOutputStream.binary[MockRecord](bos)
            aos.write(MockRecord("k3", "value3"))
            aos.flush()
            aos.close()
            bos.toByteArray
          })
      }
      val df    = spark.createDataFrame(correctedData)
      val outDf = KafkaSparkStructuredStreamingReader.selectForOneSchema(topic, df, Strict).cache
      assert(outDf.where(col("value").isNull).count() === 0)
      checkResultSchema(outDf, false)
    }

    "throw an exception parsing in STRICT mode a dataset with errors to avro" in {
      val topic = topicTest("avro")
      val df    = spark.createDataFrame(mockedSingleAvroData)

      val outDf = KafkaSparkStructuredStreamingReader.selectForOneSchema(topic, df, Strict)

      strictExceptionCheck(outDf, "avro")
    }

    "parse correctly in IGNORE mode a dataset with errors to avro" in {
      val topic = topicTest("avro")
      val df    = spark.createDataFrame(mockedSingleAvroData)
      val outDf = KafkaSparkStructuredStreamingReader.selectForOneSchema(topic, df, Ignore).cache
      assert(outDf.where(col("value").isNull).count() === 0)
      assert(outDf.where(col("value").isNotNull).count() === 2)
      checkResultSchema(outDf, isHandleMode = false)
    }

    "parse correctly in HANDLE mode a dataset with errors to avro" in {
      val topic = topicTest("avro")
      val df    = spark.createDataFrame(mockedSingleAvroData)

      val outDf = KafkaSparkStructuredStreamingReader.selectForOneSchema(topic, df, Handle).cache
      assert(outDf.where(col("raw").isNull).count() === 2)
      assert(outDf.where(col("raw").isNotNull).count() === 1)
      assert(outDf.where(col("value").isNull).count() === 1)
      assert(outDf.where(col("value").isNotNull).count() === 2)
      checkResultSchema(outDf, isHandleMode = true)
    }

  }
  "multiple schemas mode" should {
    "parse only data in correct column" in {
      val schema = BsonDocument.parse(AvroSchema[MockRecord].toString(true))
      val topic1 = TopicModel(
        "topic1.topic",
        0L,
        0,
        0,
        "plaintext",
        None,
        None,
        Some(Nil),
        false,
        schema,
        TopicCompression.Snappy
      )
      val topic2 = topic1.copy(
        name = "topic2",
        topicDataType = "json",
        topicCompression = TopicCompression.Snappy
      )
      val topic3 = topic2.copy(
        name = "topic3",
        topicDataType = "binary",
        topicCompression = TopicCompression.Snappy
      )
      val topic4 = topic2.copy(
        name = "topic4",
        topicDataType = "avro",
        topicCompression = TopicCompression.Snappy
      )
      val topic5 = topic4.copy(
        name = "topic5",
        topicDataType = "avro",
        schema = BsonDocument.parse(AvroSchema[MockRecord2].toString(true)),
        topicCompression = TopicCompression.Snappy
      )
      val df = spark
        .createDataset(
          Seq(
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = """{"key":"1", "raw":"valore"}""".getBytes(StandardCharsets.UTF_8),
              headers = Array.empty,
              topic = topic1.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            ),
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = """{"key":"1", "raw":"valore"}""".getBytes(StandardCharsets.UTF_8),
              headers = Array.empty,
              topic = topic2.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            ),
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = """asfdasdgasffa""".getBytes(StandardCharsets.UTF_8),
              headers = Array.empty,
              topic = topic3.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            ),
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = {
                val bos = new ByteArrayOutputStream()
                val aos = AvroOutputStream.binary[MockRecord](bos)
                aos.write(MockRecord("k", "v"))
                aos.flush()
                aos.close()
                bos.toByteArray
              },
              headers = Array.empty,
              topic = topic4.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            ),
            KafkaFakeRecord(
              key = "1".getBytes(StandardCharsets.UTF_8),
              raw = {
                val bos = new ByteArrayOutputStream()
                val aos = AvroOutputStream.binary[MockRecord2](bos)
                aos.write(MockRecord2("z"))
                aos.flush()
                aos.close()
                bos.toByteArray
              },
              headers = Array.empty,
              topic = topic5.name,
              partition = 1,
              offset = 0L,
              timestamp = new Timestamp(0L),
              timestampType = 0
            )
          )
        )
        .toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(Seq(topic1, topic2, topic3, topic4, topic5), df, Strict)

      assert(outDF.where(col(topicNameToColumnName(topic1.name)).isNotNull).count() === 1)
      assert(outDF.where(col(topic2.name).isNotNull).count() === 1)
      assert(outDF.where(col(topic3.name).isNotNull).count() === 1)
      assert(outDF.where(col(topic4.name).isNotNull).count() === 1)
      assert(outDF.where(col(topic5.name).isNotNull).count() === 1)
      assert(outDF.where(col(topicNameToColumnName(topic1.name)).isNull).count() === 4)
      assert(outDF.where(col(topic2.name).isNull).count() === 4)
      assert(outDF.where(col(topic3.name).isNull).count() === 4)
      assert(outDF.where(col(topic4.name).isNull).count() === 4)
      assert(outDF.where(col(topic5.name).isNull).count() === 4)
    }

    "parse Strict and Ignore mode correctly" in {
      val df = spark.createDataset(mockedMultiTopicData).toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(multiTopicSeq, df, Strict)
        .cache

      multiTopicSeq.map(t => col(t.name)).foreach { c =>
        assert(outDF.where(c.isNotNull).count() === 1)
        assert(outDF.where(c.isNull).count() === 3)
      }

      val ignoreOut = KafkaSparkStructuredStreamingReader.selectForMultipleSchema(multiTopicSeq, df, Strict).cache
      assert(ignoreOut.exceptAll(outDF).isEmpty)
      assert(outDF.exceptAll(ignoreOut).isEmpty)
    }

    "parse Handle mode correctly" in {
      val df = spark.createDataset(mockedMultiTopicData).toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(multiTopicSeq, df, Handle)
        .cache
      val rawCol = col("raw")

      multiTopicSeq.foreach { t =>
        val name = t.name
        assert(outDF.where(rawCol.isNull && col(name).isNotNull).count() === 1)
      }
    }

    "break when parsing error on json and Strict mode" in {
      val editedSample = mockedMultiTopicData match {
        case d1 :: tail =>
          val editD1 = d1.copy(raw = """{"key":"1",,malformed,,, "v1":"valore"}""".getBytes(StandardCharsets.UTF_8))
          editD1 +: tail
      }
      val df = spark.createDataset(editedSample).toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(multiTopicSeq, df, Strict)
      strictExceptionCheck(outDF, "json")
    }

    "filter when parsing error on json and Ignore mode" in {
      val editedSample = mockedMultiTopicData match {
        case d1 :: tail =>
          val editD1 = d1.copy(raw = """{"key":"1",,malformed,,, "v1":"valore"}""".getBytes(StandardCharsets.UTF_8))
          editD1 +: tail
      }
      val df = spark.createDataset(editedSample).toDF()

      val outDF = KafkaSparkStructuredStreamingReader.selectForMultipleSchema(multiTopicSeq, df, Ignore)
      multiTopicSeq.tail.map(t => col(t.name)).foreach { c =>
        assert(outDF.where(c.isNotNull).count() === 1)
        assert(outDF.where(c.isNull).count() === 2)
      }
      val cjson = col(multiTopicSeq.head.name)
      assert(outDF.where(cjson.isNotNull).count() === 0)
      assert(outDF.where(cjson.isNull).count() === 3)
    }

    "return raw when parsing error on json and Handle mode" in {
      val editedSample = mockedMultiTopicData match {
        case d1 :: tail =>
          val editD1 = d1.copy(raw = """{"key":"1",,malformed,,, "v1":"valore"}""".getBytes(StandardCharsets.UTF_8))
          editD1 +: tail
      }
      val df               = spark.createDataset(editedSample).toDF()
      val rawCol           = col("raw")
      val topicJsonColName = s"${multiTopicSeq.head.name}"
      val outDF            = KafkaSparkStructuredStreamingReader.selectForMultipleSchema(multiTopicSeq, df, Handle).cache()

      multiTopicSeq.tail.foreach { t =>
        val name = t.name
        assert(outDF.where(rawCol.isNull && col(name).isNotNull).count() === 1)
      }
      assert(outDF.where(rawCol.isNotNull && col(topicJsonColName).isNull).count() === 1)
    }

    "break when parsing error on avro and Strict mode" in {
      val editedSample = mockedMultiTopicData match {
        case start :+ d4 =>
          val editD4 = d4.copy(raw = Array(5))
          start :+ editD4
      }
      val df = spark.createDataset(editedSample).toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(multiTopicSeq, df, Strict)
      strictExceptionCheck(outDF, "avro")
    }

    "filter when parsing error on avro and Ignore mode" in {
      val editedSample = mockedMultiTopicData match {
        case start :+ d4 =>
          val editD4 = d4.copy(raw = Array(5))
          start :+ editD4
      }
      val df = spark.createDataset(editedSample).toDF()

      val outDF = KafkaSparkStructuredStreamingReader.selectForMultipleSchema(multiTopicSeq, df, Ignore).cache()

      multiTopicSeq.dropRight(1).map(t => col(t.name)).foreach { c =>
        assert(outDF.where(c.isNotNull).count() === 1)
        assert(outDF.where(c.isNull).count() === 2)
      }
      val cavro = col(multiTopicSeq.last.name)
      assert(outDF.where(cavro.isNotNull).count() === 0)
      assert(outDF.where(cavro.isNull).count() === 3)
    }

    "return raw when parsing error on avro and Handle mode" in {
      val editedSample = mockedMultiTopicData match {
        case start :+ d4 =>
          val editD4 = d4.copy(raw = Array(5))
          start :+ editD4
      }
      val df = spark.createDataset(editedSample).toDF()

      val rawCol           = col("raw")
      val topicAvroColName = s"${multiTopicSeq.last.name}"
      val outDF            = KafkaSparkStructuredStreamingReader.selectForMultipleSchema(multiTopicSeq, df, Handle).cache()

      multiTopicSeq.dropRight(1).foreach { t =>
        val name = t.name
        assert(outDF.where(rawCol.isNull && col(name).isNotNull).count() === 1)
      }
      assert(outDF.where(rawCol.isNotNull && col(topicAvroColName).isNull).count() === 1)
    }

    "go ahead for null plaintext and binary messages with Strict and Ignore mode" in {
      val Seq(t1, t2, t3, t4) = multiTopicSeq
      val editedSample = mockedMultiTopicData match {
        case d1 :: d2 :: d3 :: d4 :: _ =>
          val editD2 = d2.copy(raw = null)
          val editD3 = d3.copy(raw = null)
          Seq(d1, editD2, editD3, d4)
      }
      val df = spark.createDataset(editedSample).toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(multiTopicSeq, df, Strict)
        .cache
      assert(outDF.where(col(t1.name).isNotNull).count() === 1)
      assert(outDF.where(col(t4.name).isNotNull).count() === 1)
      assert(outDF.where(col(t1.name).isNull).count() === 3)
      assert(outDF.where(col(t4.name).isNull).count() === 3)
      assert(outDF.where(col(t2.name).isNull).count() === 4)
      assert(outDF.where(col(t3.name).isNull).count() === 4)
      val ignoreOut = KafkaSparkStructuredStreamingReader.selectForMultipleSchema(multiTopicSeq, df, Strict).cache
      assert(ignoreOut.exceptAll(outDF).isEmpty)
      assert(outDF.exceptAll(ignoreOut).isEmpty)
    }

    "null raw for null plaintext and binary messages with Handle mode" in {
      val Seq(t1, t2, t3, t4) = multiTopicSeq

      val editedSample = mockedMultiTopicData match {
        case d1 :: d2 :: d3 :: d4 :: _ =>
          val editD2 = d2.copy(raw = null)
          val editD3 = d3.copy(raw = null)
          Seq(d1, editD2, editD3, d4)
      }
      val df = spark.createDataset(editedSample).toDF()

      val outDF = KafkaSparkStructuredStreamingReader
        .selectForMultipleSchema(multiTopicSeq, df, Handle)
        .cache
      val rawCol = col("raw")
      Seq(t1, t4).foreach { t =>
        val name = t.name
        assert(outDF.where(rawCol.isNull && col(name).isNotNull).count() === 1)
      }
      Seq(t2, t3).foreach { t =>
        val name = t.name
        assert(outDF.where(rawCol.isNull && col(name).isNull && col("kafkametadata.topic") === name).count() === 1)
      }
    }
  }

  def strictExceptionCheck(df: DataFrame, string2contain: String) = {
    Try(df.collect) match {
      case Success(_)                 => fail("a spark exception should be thrown, not a Success")
      case Failure(e: SparkException) => assert(e.getCause.toString.contains(string2contain))
      case Failure(e)                 => fail(s"a spark exception should be thrown, not $e ")
    }
  }
  def checkResultSchema(df: DataFrame, isHandleMode: Boolean): Unit = {
    val parsedValue = StructType(
      Array(StructField("key", StringType, nullable = false), StructField("v1", StringType, nullable = false))
    )
    val handleSchema = StructType(
      Array(
        StructField("raw", BinaryType),
        StructField("value", parsedValue)
      )
    )
    val toCheck = df.drop("kafkaMetadata").schema.toString().replaceAll(",true", ",false")
    if (isHandleMode) {
      assert(handleSchema.toString().replaceAll(",true", ",false") == toCheck)
    } else {
      assert(parsedValue.toString().replaceAll(",true", ",false") == toCheck)
    }
  }
  def topicTest(_type: String, name: String = "topic-test"): TopicModel = TopicModel(
    name,
    0L,
    0,
    0,
    _type,
    None,
    None,
    Some(Nil),
    useAvroSchemaManager = false,
    BsonDocument.parse(AvroSchema[MockRecord].toString(true)),
    TopicCompression.Snappy
  )

  val multiTopicSeq = Seq(
    topicTest(TopicDataTypes.JSON, "topic_json"),
    topicTest(TopicDataTypes.BINARY, "topic_binary"),
    topicTest(TopicDataTypes.PLAINTEXT, "topic_plain"),
    topicTest(TopicDataTypes.AVRO, "topic_avro")
  )

  val mockedMultiTopicData = {
    val Seq(t1, t2, t3, t4) = multiTopicSeq
    Seq(
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = """{"key":"1", "v1":"valore"}""".getBytes(StandardCharsets.UTF_8),
        headers = Array.empty,
        topic = t1.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = """binary_test""".getBytes(StandardCharsets.UTF_8),
        headers = Array.empty,
        topic = t2.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = """plaintext_test""".getBytes(StandardCharsets.UTF_8),
        headers = Array.empty,
        topic = t3.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = {
          val bos = new ByteArrayOutputStream()
          val aos = AvroOutputStream.binary[MockRecord](bos)
          aos.write(MockRecord("k", "v"))
          aos.flush()
          aos.close()
          bos.toByteArray
        },
        headers = Array.empty,
        topic = t4.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      )
    )
  }
  val mockedSingleJsonData = {
    val topic = topicTest("json")
    Seq(
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = """{"key":"1", "v1":"value"}""".getBytes(StandardCharsets.UTF_8),
        headers = Array.empty,
        topic = topic.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = """{"key":"2", "v1":"value2"}""".getBytes(StandardCharsets.UTF_8),
        headers = Array.empty,
        topic = topic.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = """{"key":"3",,, "v1":"value3"}""".getBytes(StandardCharsets.UTF_8),
        headers = Array.empty,
        topic = topic.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      )
    )
  }
  val mockedSingleAvroData = {
    val topic = topicTest("avro")
    Seq(
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = {
          val bos = new ByteArrayOutputStream()
          val aos = AvroOutputStream.binary[MockRecord](bos)
          aos.write(MockRecord("k", "value"))
          aos.flush()
          aos.close()
          bos.toByteArray
        },
        headers = Array.empty,
        topic = topic.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = {
          val bos = new ByteArrayOutputStream()
          val aos = AvroOutputStream.binary[MockRecord](bos)
          aos.write(MockRecord("k2", "value2"))
          aos.flush()
          aos.close()
          bos.toByteArray
        },
        headers = Array.empty,
        topic = topic.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      ),
      KafkaFakeRecord(
        key = "1".getBytes(StandardCharsets.UTF_8),
        raw = Array(5),
        headers = Array.empty,
        topic = topic.name,
        partition = 1,
        offset = 0L,
        timestamp = new Timestamp(0L),
        timestampType = 0
      )
    )
  }
}
case class MockRecord(key: String, v1: String)
case class MockRecord2(raw: String)

case class KafkaFakeRecord(
    key: Array[Byte],
    raw: Array[Byte],
    headers: Array[(String, Array[Byte])],
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: java.sql.Timestamp,
    timestampType: Int
)
