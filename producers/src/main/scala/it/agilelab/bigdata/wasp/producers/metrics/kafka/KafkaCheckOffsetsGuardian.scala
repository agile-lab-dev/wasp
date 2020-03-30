package it.agilelab.bigdata.wasp.producers.metrics.kafka

import java.util.Date

import akka.actor.{Actor, ActorRef, Props}
import it.agilelab.bigdata.wasp.core.kafka.WaspKafkaWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.KafkaConfigModel
import it.agilelab.bigdata.wasp.producers.StopMainTask

import scala.collection.mutable

object KafkaCheckOffsetsGuardian {
  val name = "KafkaCheckOffsetsGuardian"

  def props(waspKafkaConfig: KafkaConfigModel): Props = {
    val kafkaProps = WaspKafkaWriter.createConfig(
      brokers = waspKafkaConfig.connections.map(_.toString).toSet,
      batchSendSize = waspKafkaConfig.batch_send_size,
      acks = waspKafkaConfig.acks,
      keySerializerFqcn = "org.apache.kafka.common.serialization.ByteArraySerializer",
      serializerFqcn = "org.apache.kafka.common.serialization.ByteArraySerializer",
      others = waspKafkaConfig.others
    )
    kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    val childActorFactory: String => Props =
      (topicName: String) => Props(new KafkaCheckOffsetsActor(topicName, kafkaProps))

    Props(new KafkaCheckOffsetsGuardian(childActorFactory))
  }
}

class KafkaCheckOffsetsGuardian(childActorFactory: String => Props) extends Actor with Logging {

  // we keep all child actors here, we add a new actor every time the offsets of a new topic are requested
  private val childActors = mutable.Map[String, ActorRef]()

  override def receive: Receive = {
    case r@KafkaOffsetsRequest(_, topic, ts) =>
      logger.debug(s"Received a request for the offsets of topic $topic at ${new Date(ts)}")
      childActors.getOrElseUpdate(topic, spawnKafkaCheckOffsetsActor(topic)) ! r
    case KafkaOffsetActorAlive =>
      logger.debug(s"Replying to ${sender()} that we are alive")
      sender() ! KafkaOffsetActorAlive
    case StopMainTask =>
      logger.info(s"Stopping all childrens of ${KafkaCheckOffsetsGuardian.name}")
      childActors.values.foreach { child =>
        logger.info(s"Stopping child $child")
        child ! StopMainTask
      }
  }

  protected def spawnKafkaCheckOffsetsActor(topic: String): ActorRef = {
    val aRef = context.actorOf(childActorFactory(topic))
    logger.info(s"Created actor KafkaCheckOffsetsActor for topic $topic")
    aRef
  }
}
