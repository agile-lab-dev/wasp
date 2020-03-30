package it.agilelab.bigdata.wasp.producers.metrics.kafka

import java.util.{Date, Properties}

import akka.actor.Actor
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.producers.StopMainTask
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

class KafkaCheckOffsetsActor(topicName: String,
                             kafkaProps: Properties) extends Actor with Logging {

  private[this] var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _

  override def preStart(): Unit = {
    super.preStart()
    consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaProps)
    consumer.subscribe(java.util.Arrays.asList(topicName))
  }

  override def postStop(): Unit = {
    IOUtils.closeQuietly(consumer)
    super.postStop()
  }

  override def receive: Receive = {
    case KafkaOffsetsRequest(replyTo, topic, ts) =>
      if (topic != topicName) {
        replyTo ! WrongKafkaOffsetsRequest(s"topic $topic it's different from the configured topic: $topicName")
      } else {
        logger.debug(s"Fetching latest offsets for $topicName, request was made at: ${new Date(ts)}")
        val partitions = consumer.partitionsFor(topic).asScala.map { info =>
          new TopicPartition(info.topic(), info.partition())
        }
        val offs = consumer.endOffsets(partitions.asJava).asScala.map { case (k, v) =>
          k.partition() -> v.toLong
        }.toMap
        replyTo ! KafkaOffsets(topic, offs, System.currentTimeMillis())
      }
    case StopMainTask =>
      logger.info(s"Shutting down actor: KafkaCheckOffsetsActor($topicName)")
      IOUtils.closeQuietly(consumer)
  }

}
