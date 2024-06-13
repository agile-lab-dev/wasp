package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import it.agilelab.bigdata.wasp.spark.plugins.telemetry.CompatibilityTelemetryPluginProducer.load
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Future

object TelemetryPluginProducer {

  @transient private lazy val cache = CompatibilityTelemetryPluginProducer.getCacheBuilder(load())

  def send(kafkaConfig: TelemetryMetadataProducerConfig, key: String, value: String): Future[RecordMetadata] = {
    val topicName = kafkaConfig.telemetry.topicName.toLowerCase() + ".topic"
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topicName,
      key.getBytes(StandardCharsets.UTF_8),
      value.getBytes(StandardCharsets.UTF_8))

    cache.get(kafkaConfig).send(record)
  }


}

