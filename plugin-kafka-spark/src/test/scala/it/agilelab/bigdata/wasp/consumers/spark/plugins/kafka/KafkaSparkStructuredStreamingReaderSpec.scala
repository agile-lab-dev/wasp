package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.TopicModelUtils.topicNameToColumnName
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.{TopicCompression, TopicModel}
import org.apache.spark.sql.functions._
import org.bson.BsonDocument
import org.scalatest.FlatSpec

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

class KafkaSparkStructuredStreamingReaderSpec extends FlatSpec with SparkSuite {

  it should "parse only data in correct column when in multi-mode" in {
    import spark.implicits._
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
            value = """{"key":"1", "value":"valore"}""".getBytes(StandardCharsets.UTF_8),
            headers = Array.empty,
            topic = topic1.name,
            partition = 1,
            offset = 0L,
            timestamp = new Timestamp(0L),
            timestampType = 0
          ),
          KafkaFakeRecord(
            key = "1".getBytes(StandardCharsets.UTF_8),
            value = """{"key":"1", "value":"valore"}""".getBytes(StandardCharsets.UTF_8),
            headers = Array.empty,
            topic = topic2.name,
            partition = 1,
            offset = 0L,
            timestamp = new Timestamp(0L),
            timestampType = 0
          ),
          KafkaFakeRecord(
            key = "1".getBytes(StandardCharsets.UTF_8),
            value = """asfdasdgasffa""".getBytes(StandardCharsets.UTF_8),
            headers = Array.empty,
            topic = topic3.name,
            partition = 1,
            offset = 0L,
            timestamp = new Timestamp(0L),
            timestampType = 0
          ),
          KafkaFakeRecord(
            key = "1".getBytes(StandardCharsets.UTF_8),
            value = {
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
            value = {
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
      .selectForMultipleSchema(Seq(topic1, topic2, topic3, topic4, topic5), df)

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

  it should "parse only data in correct column when in single mode" in {
    import spark.implicits._
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
            value = """{"key":"1", "value":"valore"}""".getBytes(StandardCharsets.UTF_8),
            headers = Array.empty,
            topic = topic1.name,
            partition = 1,
            offset = 0L,
            timestamp = new Timestamp(0L),
            timestampType = 0
          ),
          KafkaFakeRecord(
            key = "1".getBytes(StandardCharsets.UTF_8),
            value = """{"key":"1", "value":"valore"}""".getBytes(StandardCharsets.UTF_8),
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
      .selectForOneSchema(topic1, df)

    assert(outDF.where(col("value").isNull.or(col("key").isNull)).count() === 0)
  }
}
case class MockRecord(key: String, value: String)
case class MockRecord2(key: String)

case class KafkaFakeRecord(
    key: Array[Byte],
    value: Array[Byte],
    headers: Array[(String, Array[Byte])],
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: java.sql.Timestamp,
    timestampType: Int
)
