package it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.models.TopicModel

class ImplKafkaThroughputProducer(kafka_router: ActorRef,
                                  kafkaOffsetChecker: ActorRef,
                                  topic: Option[TopicModel],
                                  topicToCheck: String,
                                  windowSize: Long,
                                  sendMessageEveryXsamples: Int,
                                  triggerIntervalMs: Long) extends KafkaThroughputProducerActor[String](
  kafka_router, kafkaOffsetChecker, topic, topicToCheck, windowSize, sendMessageEveryXsamples, triggerIntervalMs) {

  override protected def toFinalMessage(messageSumInWindow: Long, timestamp: Long): String =
    s"count: ${messageSumInWindow} at ${new java.util.Date(timestamp)}"

  /**
    * Used when writing to topics with data type "json" or "avro"
    */
  override def generateOutputJsonMessage(input: String): String = input


  /**
    * Used when writing to topics with data type "plaintext"
    */
  override def generateOutputPlaintextMessage(input: String): String = input

  /**
    * Defines a function to extract the key to be used to identify the landing partition in kafka topic,
    * given a message of type T
    *
    * @return a function that extract the partition key as String from the T instance to be sent to kafka
    */
  override def retrievePartitionKey: String => String = _ => ""
}

class ImplKafkaThroughputGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String)
  extends KafkaThroughputProducerGuardian[String](env, producerName) {

  override protected def createActor(kafkaActor: ActorRef, topicToCheck: String, triggerInterval: Long, windowSize: Long, sendMessageEveryXsamples: Int): KafkaThroughputProducerActor[String] = {
    new ImplKafkaThroughputProducer(kafka_router, kafkaActor, associatedTopic, topicToCheck, windowSize, sendMessageEveryXsamples, triggerInterval)
  }
}