package it.agilelab.bigdata.wasp.core.kafka

import java.util.Properties

import it.agilelab.bigdata.wasp.core.WaspEvent
import it.agilelab.bigdata.wasp.core.WaspEvent.WaspMessageEnvelope
import it.agilelab.bigdata.wasp.core.models.configuration.{TinyKafkaConfig, KafkaConfigModel}
import kafka.producer.{DefaultPartitioner, KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.KafkaConfig

/** Simple producer using generic encoder and default partitioner. */
//TODO remove and use kafka connect or kafka camel
class WaspKafkaWriter[K, V](producerConfig: ProducerConfig) {

  def this(brokers: Set[String], batchSize: Int, producerType: String, serializerFqcn: String, keySerializerFqcn: String, partitionerFqcn: String) =
    this(WaspKafkaWriter.createConfig(brokers, batchSize, producerType, serializerFqcn, keySerializerFqcn, partitionerFqcn))

  def this(config: KafkaConfig) =
    this(WaspKafkaWriter.defaultConfig(config))

  def this(conf: KafkaConfigModel) = this(WaspKafkaWriter.createConfig(
    conf.connections.map(x => x.toString).toSet, conf.batch_send_size, "async", conf.default_encoder, conf.encoder_fqcn, conf.partitioner_fqcn))

  def this(conf: TinyKafkaConfig) = this(WaspKafkaWriter.createConfig(
    conf.connections.map(x => x.toString).toSet, conf.batch_send_size, "async", conf.default_encoder, conf.encoder_fqcn, conf.partitioner_fqcn))

  import WaspEvent._

  private val producer = new Producer[K, V](producerConfig)

  /** Sends the data, partitioned by key to the topic. */
  def send(e: WaspMessageEnvelope[K, V]): Unit =
    batchSend(e.topic, e.key, e.messages)

  /* Sends a single message. */
  def send(topic: String, key: K, message: V): Unit =
    batchSend(topic, key, Seq(message))

  def batchSend(topic: String, key: K, batch: Seq[V]): Unit = {
    val messages = batch map (msg => new KeyedMessage[K, V](topic, key, msg))
    producer.send(messages.toArray: _*)
  }

  def close(): Unit = producer.close()

}


object WaspKafkaWriter {

  def createConfig(brokers: Set[String], batchSize: Int, producerType: String, serializerFqcn: String, keySerializerFqcn: String, partitionerFqcn: String): ProducerConfig = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("serializer.class", serializerFqcn)
    props.put("key.serializer.class", keySerializerFqcn)
    props.put("partitioner.class", partitionerFqcn)
    props.put("producer.type", producerType)
    props.put("request.required.acks", "1")
    props.put("batch.num.messages", batchSize.toString)
    new ProducerConfig(props)
  }

  def defaultConfig(config: KafkaConfig): ProducerConfig =
    createConfig(Set(s"${config.hostName}:${config.port}"), 100, "async", classOf[StringEncoder].getName, classOf[StringEncoder].getName, classOf[DefaultPartitioner].getName)
}