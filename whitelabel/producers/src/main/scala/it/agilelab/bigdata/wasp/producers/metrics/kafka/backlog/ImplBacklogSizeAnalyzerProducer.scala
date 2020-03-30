package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog


import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.TopicModel

class ImplBacklogSizeAnalyzerProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL},
                                              producerName: String) extends BacklogSizeAnalyzerProducerGuardian[String](env, producerName) {
  override protected def createActor(kafka_router: ActorRef, kafkaOffsetChecker: ActorRef, topic: Option[TopicModel], topicToCheck: String, etlName: String): BacklogSizeAnalyzerProducerActor[String] = {
    new ImplBacklogSizeAnalyzerProducerActor(kafka_router, kafkaOffsetChecker, topic, topicToCheck, etlName)
  }
}

class ImplBacklogSizeAnalyzerProducerActor(kafka_router: ActorRef,
                                           kafkaOffsetChecker: ActorRef,
                                           topic: Option[TopicModel],
                                           topicToCheck: String,
                                           etlName: String
                                          ) extends BacklogSizeAnalyzerProducerActor[String](kafka_router, kafkaOffsetChecker, topic, topicToCheck, etlName) {
  override def toFinalMessage(i: BacklogInfo): String = i.toString

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
