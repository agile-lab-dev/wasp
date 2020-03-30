package it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import it.agilelab.bigdata.wasp.producers.metrics.kafka._

import scala.concurrent.duration.FiniteDuration


class TestKafkaThroughputGuardian extends KafkaThroughputProducerGuardian[String](
  Env,
  "TestKafkaThroughputGuardian",
  _.actorOf(Props(
    new KafkaCheckOffsetsGuardian((topic: String) => Props(new TestKafkaCheckKafkaOffset(topic))))
  ),
  FiniteDuration(5, TimeUnit.MILLISECONDS)
) {


  override def initialize(): Either[String, Unit] = {
    kafka_router = context.actorOf(Props(new TestKafkaRouter()), "TestKafkaRouter")
    startChildActors()
    context become initialized
    Right(())
  }

  override protected def createActor(kafkaActor: ActorRef,
                                     topicToCheck: String,
                                     triggerInterval: Long,
                                     windowSize: Long,
                                     sendMessageEveryXsamples: Int): KafkaThroughputProducerActor[String] = {
    new TestKafkaThroughputProducerActor(kafka_router, kafkaActor, topicToCheck, triggerInterval, windowSize, sendMessageEveryXsamples)
  }

  override protected def kafkaThroughputConfigs(): Either[String, List[KafkaThroughputConfig]] = {
    Right(List(KafkaThroughputConfig(Constants.throughputTestTopic, Constants.TriggerInterval, Constants.TriggerInterval * 10, 1)))
  }
}

class TestKafkaThroughputProducerActor(kafkaRouter: ActorRef,
                                       kafkaActor: ActorRef,
                                       topicToCheck: String,
                                       triggerInterval: Long,
                                       windowSize: Long,
                                       sendMessageEveryXsamples: Int) extends
  KafkaThroughputProducerActor[String](kafkaRouter,
    kafkaActor, None, topicToCheck, windowSize, sendMessageEveryXsamples, triggerInterval) {

  var counter: Int = 0

  override protected def toFinalMessage(messageSumInWindow: Long, timestamp: Long): String =
    s"$messageSumInWindow:$counter"

  override def generateOutputJsonMessage(input: String): String = input

  override def generateOutputPlaintextMessage(input: String): String = input

  override def generateOutputBinaryMessage(input: String): Array[Byte] = input.getBytes(StandardCharsets.UTF_8)

  override def sendMessage(input: String): Unit = {
    counter += 1
    Constants.testThroughputActor ! input
  }


  override def preStart(): Unit = {
    super.preStart()
    Constants.throughputProducerPool(topicToCheck) = this
  }

  override def retrievePartitionKey: String => String = identity
}