package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import it.agilelab.bigdata.wasp.models.{PipegraphModel, TopicModel}
import it.agilelab.bigdata.wasp.producers.metrics.kafka.{Constants, Env, KafkaCheckOffsetsGuardian, TestKafkaCheckKafkaOffset, TestKafkaRouter}

import scala.concurrent.duration.FiniteDuration

class TestBacklogSizeAnalyzerProducerActor(testActor: ActorRef,
                                           kafka_router: ActorRef,
                                           kafkaOffsetChecker: ActorRef,
                                           topic: Option[TopicModel],
                                           topicToCheck: String,
                                           etlName: String)
  extends BacklogSizeAnalyzerProducerActor[String](kafka_router, kafkaOffsetChecker, topic, topicToCheck, etlName) {

  var counter = 0L

  override def preStart(): Unit = {
    super.preStart()
    Constants.backlogProducerPool(etlName) = this
  }

  override def toFinalMessage(i: BacklogInfo): String = {
    s"${i.backlogSize}:${i.etlName}:${counter}"
  }

  override def sendMessage(input: String): Unit = {
    counter += 1
    testActor ! input
  }

  override def generateOutputPlaintextMessage(input: String): String = input

  override def generateOutputJsonMessage(input: String): String = input

  override def retrievePartitionKey: String => String = identity
}

class TestBacklogSizeAnalyzerProducerGuardian(testActor: ActorRef)
  extends BacklogSizeAnalyzerProducerGuardian[String](Env,
    "TestBacklogSizeAnalyzerProducerGuardian",
    _.actorOf(Props(
      new KafkaCheckOffsetsGuardian((topic: String) => Props(new TestKafkaCheckKafkaOffset(topic)))),"TestKafkaCheckKafkaOffset"
    ),
    FiniteDuration(5, TimeUnit.MILLISECONDS)) {
  override protected def createActor(kafka_router: ActorRef,
                                     kafkaOffsetChecker: ActorRef,
                                     topic: Option[TopicModel],
                                     topicToCheck: String,
                                     etlName: String): BacklogSizeAnalyzerProducerActor[String] = {
    new TestBacklogSizeAnalyzerProducerActor(testActor, kafka_router, kafkaOffsetChecker, topic, topicToCheck, etlName)
  }

  override protected def backlogAnalyzerConfigs(allPipegraphs: Map[String, PipegraphModel]): Either[String, List[BacklogAnalyzerConfig]] = {
    Right(allPipegraphs.values.map(model => BacklogAnalyzerConfig(model, model.structuredStreamingComponents)).toList)
  }

  override protected def getAllPipegraphs: Map[String, PipegraphModel] = Map(
    Constants.TestPipegraph.name -> Constants.TestPipegraph
  )

  override def initialize(): Either[String, Unit] = {
    kafka_router = context.actorOf(Props(new TestKafkaRouter()), "TestKafkaRouter")
    producer = Env.producerBL.getByName(name).get
    startChildActors()
    Right(())
  }
}