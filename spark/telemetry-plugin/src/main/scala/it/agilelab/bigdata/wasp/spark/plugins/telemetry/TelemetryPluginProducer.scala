package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.spark_project.guava.cache.{CacheBuilder, CacheLoader, LoadingCache}
import scala.collection.JavaConverters._

object TelemetryPluginProducer {

  @transient private lazy val cache: LoadingCache[TelemetryMetadataProducerConfig, KafkaProducer[Array[Byte], Array[Byte]]] = CacheBuilder
    .newBuilder()
    .build(load())


  def send(kafkaConfig: TelemetryMetadataProducerConfig, key: String, value: String): Future[RecordMetadata] = {
    val topicName = kafkaConfig.telemetry.topicName.toLowerCase() + ".topic"
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topicName,
      key.getBytes(StandardCharsets.UTF_8),
      value.getBytes(StandardCharsets.UTF_8))

    cache.get(kafkaConfig).send(record)
  }


  private def load() = new CacheLoader[TelemetryMetadataProducerConfig, KafkaProducer[Array[Byte], Array[Byte]]] {

    override def load(config: TelemetryMetadataProducerConfig): KafkaProducer[Array[Byte], Array[Byte]] = {



      val kafkaConfig = config.global

      val telemetryConfig = config.telemetry

      val connectionString = kafkaConfig.connections.map {
        conn => s"${conn.host}:${conn.port}"
      }.mkString(",")


      val props = new Properties()
      props.put("bootstrap.servers", connectionString)
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

      val notOverridableKeys = props.keySet.asScala

      val merged: Seq[(String, String)] = kafkaConfig.others ++ telemetryConfig.kafkaSettings

      val resultingConf = merged.filterNot(x => notOverridableKeys.contains(x._1))

      resultingConf.foreach {
        case (key, value) => props.put(key, value)
      }

      new KafkaProducer[Array[Byte], Array[Byte]](props)

    }
  }

}
