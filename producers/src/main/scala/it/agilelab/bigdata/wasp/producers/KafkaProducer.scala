package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorLogging}
import it.agilelab.bigdata.wasp.core.WaspEvent
import it.agilelab.bigdata.wasp.core.kafka.WaspKafkaWriter
import it.agilelab.bigdata.wasp.core.models.configuration.KafkaConfigModel
import kafka.producer.ProducerConfig

class KafkaPublisherActor(val producerConfig: ProducerConfig) extends KafkaProducerActor[String, Array[Byte]] {

  def this(conf: KafkaConfigModel) = this(WaspKafkaWriter.createConfig(
    conf.connections.map(x => x.toString).toSet, conf.batch_send_size, "async", conf.default_encoder, conf.encoder_fqcn, conf.partitioner_fqcn))
}

/** Simple producer for an Akka Actor using generic encoder and default partitioner. */
abstract class KafkaProducerActor[K, V] extends Actor with ActorLogging {

  import WaspEvent._

  def producerConfig: ProducerConfig

  private val producer = new WaspKafkaWriter[K, V](producerConfig)

  override def postStop(): Unit = {
    log.info("Shutting down producer.")
    producer.close()
  }

  def receive = {
    case e: WaspMessageEnvelope[K, V] => producer.send(e)
  }
}

