package it.agilelab.bigdata.wasp.core.kafka

import java.util.Properties

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.WaspMessageEnvelope
import it.agilelab.bigdata.wasp.models.configuration.{KafkaConfigModel, KafkaEntryConfig, TinyKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/** Simple producer using generic encoder and default partitioner. */
//TODO remove and use kafka connect or kafka camel
class WaspKafkaWriter[K, V](producerConfig: Properties) {

  def this(brokers: Set[String], batchSize: Int, acks: String , producerType: String, serializerFqcn: String, keySerializerFqcn: String) =
    this(WaspKafkaWriter.createConfig(brokers, batchSize, acks, serializerFqcn, keySerializerFqcn))

  def this(conf: KafkaConfigModel) = this(WaspKafkaWriter.createConfig(
    conf.connections.map(x => x.toString).toSet, conf.batch_send_size, conf.acks, conf.encoder_fqcn, conf.encoder_fqcn))

  def this(conf: TinyKafkaConfig) = this(WaspKafkaWriter.createConfig(
    conf.connections.map(x => x.toString).toSet, conf.batch_send_size, conf.acks, conf.encoder_fqcn, conf.encoder_fqcn))

  //logger.info(s"Kafka Producer configuration $producerConfig")
  private val producer = new KafkaProducer[K, V](producerConfig)

  /** Sends the data, partitioned by key to the topic. */
  def send(e: WaspMessageEnvelope[K, V]) = batchSend(e.topic, e.key, e.messages)

  /* Sends a single message. */
  def send(topic: String, key: K, message: V) = batchSend(topic, key, Seq(message))

  def batchSend(topic: String, key: K, batch: Seq[V]) = {
    batch
      .map(msg => new ProducerRecord[K, V](topic, key, msg))
      .foreach(producer.send)
  }

  def close(): Unit = producer.close()
}

object WaspKafkaWriter {

  def createConfig(brokers: Set[String], batchSendSize: Int, acks: String,
                   keySerializerFqcn: String, serializerFqcn: String,
                   others: Seq[KafkaEntryConfig] = Seq()): Properties = {

    val props = new Properties()
    props.put("bootstrap.servers", brokers.mkString(","))
    props.put("value.serializer", serializerFqcn)
    props.put("key.serializer", keySerializerFqcn)
    props.put("batch.size", batchSendSize.toString)
    props.put("acks", acks)

    others.foreach(v => {
      props.put(v.key, v.value)
    })

    props
  }
}