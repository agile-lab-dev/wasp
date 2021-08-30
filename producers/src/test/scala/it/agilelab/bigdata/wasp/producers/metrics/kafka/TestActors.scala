package it.agilelab.bigdata.wasp.producers.metrics.kafka

import akka.actor.{Actor, ActorRef}
import it.agilelab.bigdata.wasp.repository.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian
import it.agilelab.bigdata.wasp.models.{
  DatastoreModel,
  PipegraphModel,
  ProducerModel,
  StreamingReaderModel,
  StructuredStreamingETLModel,
  TopicModel,
  WriterModel
}
import it.agilelab.bigdata.wasp.producers.StopMainTask
import it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog.TestBacklogSizeAnalyzerProducerActor
import it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput.TestKafkaThroughputProducerActor
import org.bson.BsonDocument

object Env {
  val producerBL: ProducerBL = new ProducerBL {
    override def getByName(name: String): Option[ProducerModel] =
      Some(ProducerModel(name, name, Some("ATopics"), false, None, false, false))

    override def getActiveProducers(isActive: Boolean): Seq[ProducerModel] = ???

    override def getSystemProducers: Seq[ProducerModel] = ???

    override def getNonSystemProducers: Seq[ProducerModel] = ???

    override def getByTopicName(name: String): Seq[ProducerModel] = ???

    override def getTopic(topicBL: TopicBL, producerModel: ProducerModel): Option[TopicModel] =
      Some(
        TopicModel(
          producerModel.topicName.get,
          System.currentTimeMillis(),
          1,
          1,
          "plaintext",
          None,
          None,
          None,
          false,
          new BsonDocument()
        )
      )

    override def getAll: Seq[ProducerModel] = ???

    override def update(producerModel: ProducerModel): Unit = ()

    override def persist(producerModel: ProducerModel): Unit = ???

    override def upsert(producerModel: ProducerModel): Unit = ???

    override def insertIfNotExists(producerModel: ProducerModel): Unit = ???

  }
  val topicBL = new TopicBL() {
    override def getByName(name: String): Option[DatastoreModel] = ???

    override def getAll: Seq[DatastoreModel] = ???

    override def persist(topicModel: DatastoreModel): Unit = ???

    override def upsert(topicModel: DatastoreModel): Unit = ???

    override def insertIfNotExists(topicDatastoreModel: DatastoreModel): Unit = ???
  }
}

object Constants {

  import scala.collection.mutable

  val offsetCheckerPool: mutable.Map[String, TestKafkaCheckKafkaOffset]              = mutable.Map.empty
  val throughputProducerPool: mutable.Map[String, TestKafkaThroughputProducerActor]  = mutable.Map.empty
  val backlogProducerPool: mutable.Map[String, TestBacklogSizeAnalyzerProducerActor] = mutable.Map.empty
  val throughputTestTopic                                                            = "throughputTestTopic"
  val backlogTestTopic                                                               = "backlogTestTopic"
  val TestEtl = StructuredStreamingETLModel(
    "testEtl",
    "default",
    StreamingReaderModel.kafkaReader(
      "",
      TopicModel(backlogTestTopic, 0L, 1, 1, "plaintext", None, None, None, false, new BsonDocument()),
      None
    ),
    Nil,
    WriterModel.consoleWriter("console"),
    Nil,
    None,
    None
  )
  val TestPipegraph = PipegraphModel(
    name = "testPipegraph",
    description = "",
    owner = "",
    isSystem = false,
    creationTime = 0L,
    legacyStreamingComponents = Nil,
    structuredStreamingComponents = TestEtl :: Nil,
    rtComponents = Nil
  )

  val sourceId                      = BaseConsumersMasterGuadian.generateUniqueComponentName(Constants.TestPipegraph, Constants.TestEtl)
  val TriggerInterval: Long         = 50L
  var testThroughputActor: ActorRef = _
}

class TestKafkaRouter extends Actor {
  override def receive: Receive = {
    case _ => ()
  }
}

class TestKafkaCheckKafkaOffset(topicName: String) extends Actor {
  var offsets: Map[Int, Long] = Map(0 -> 0L)

  override def preStart(): Unit = {
    Constants.offsetCheckerPool(topicName) = this
  }

  override def receive: Receive = {
    case KafkaOffsetsRequest(replyTo, topic, _) =>
      if (topic != topicName) {
        replyTo ! WrongKafkaOffsetsRequest(s"topic $topic it's different from the configured topic: $topicName")
      } else {
        replyTo ! KafkaOffsets(topic, offsets, System.currentTimeMillis())
      }
    case StopMainTask =>
      println("Stopping")
    case KafkaOffsetActorAlive => sender() ! KafkaOffsetActorAlive

  }
}
