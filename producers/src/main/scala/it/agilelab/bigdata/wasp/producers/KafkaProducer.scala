package it.agilelab.bigdata.wasp.producers

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import it.agilelab.bigdata.wasp.core.kafka.WaspKafkaWriter
import it.agilelab.bigdata.wasp.core.messages.WaspMessageEnvelope
import it.agilelab.bigdata.wasp.core.models.configuration.KafkaConfigModel

class KafkaPublisherActor(val producerConfig: Properties) extends KafkaProducerActor[String, Array[Byte]] {

  def this(conf: KafkaConfigModel) = this(WaspKafkaWriter.createConfig(
    conf.connections.map(x => x.toString).toSet, conf.batch_send_size, conf.key_encoder_fqcn, conf.encoder_fqcn, conf.others))
}

/** Simple producer for an Akka Actor using generic encoder and default partitioner. */
abstract class KafkaProducerActor[K, V] extends Actor with ActorLogging {

  def producerConfig: Properties

  private val producer = new WaspKafkaWriter[K, V](producerConfig)

  override def postStop(): Unit = {
    log.info("Shutting down producer.")
    producer.close()
  }

  override def receive = {
    case e: WaspMessageEnvelope[K, V] =>
      producer.send(e)
  }
}