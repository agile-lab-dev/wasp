package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.bigdata.wasp.core.messages.{Start, Stop, TelemetryMessageSource, TelemetryMessageSourcesSummary}
import it.agilelab.bigdata.wasp.producers.metrics.kafka.Constants
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration.FiniteDuration

class BacklogSizeAnalyzerSpec
    extends TestKit(
      ActorSystem(
        "BacklogSizeAnalyzerSpec",
        ConfigFactory
          .load()
          .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0))
      )
    )
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  it should "start, calculate backlog and then stop gracefully" in {

    val backlogGuardian = system.actorOf(
      Props(new TestBacklogSizeAnalyzerProducerGuardian(testActor)),
      "TestBacklogSizeAnalyzerProducerGuardian"
    )
    backlogGuardian ! Start
    expectMsg(FiniteDuration(20, TimeUnit.SECONDS), Right(()))
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 0, 0)
    while (!Constants.offsetCheckerPool.contains(Constants.backlogTestTopicName)) {}
    Constants.offsetCheckerPool(Constants.backlogTestTopicName).offsets = Map(0 -> 0L)
    expectMsg("0:testEtl:0")
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 0, 0)
    expectMsg("0:testEtl:1")
    Constants.offsetCheckerPool(Constants.backlogTestTopicName).offsets = Map(0 -> 5L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 0, 0)
    expectMsg("5:testEtl:2")
    Constants.offsetCheckerPool(Constants.backlogTestTopicName).offsets = Map(0 -> 5L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 0, 3)
    expectMsg("2:testEtl:3")
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 3, 5)
    expectMsg("0:testEtl:4")
    Constants.offsetCheckerPool(Constants.backlogTestTopicName).offsets = Map(0 -> 1000L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 5, 5)
    expectMsg("995:testEtl:5")
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 5, 500)
    expectMsg("500:testEtl:6")
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.backlogTestTopicName, 5, 1000)
    expectMsg("0:testEtl:7")
    backlogGuardian ! Stop
    expectMsg(Right(()))
    expectNoMsg()
  }

  it should "start, calculate backlog on multi topic model and then stop gracefully" in {

    val etlName = Constants.TestMultiTopicModelEtl.name

    /*
    The guardian will load a pipegraph with a multi topic model ad input.
    The multi topic model is composed from multiTopic1 and multiTopic2
     */
    val backlogGuardian = system.actorOf(
      Props(new TestMultiTopicBacklogSizeAnalyzerProducerGuardian(testActor)),
      "TestMultiTopicBacklogSizeAnalyzerProducerGuardian"
    )

    backlogGuardian ! Start
    expectMsg(FiniteDuration(20, TimeUnit.SECONDS), Right(()))

    /*
    Here we start sending message to the topic composing the pipegraph just loaded.
     */

    // TOPIC 1
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 0, 0)
    while (!Constants.offsetCheckerPool.contains(Constants.multiTopic1)) {}
    Constants.offsetCheckerPool(Constants.multiTopic1).offsets = Map(0 -> 0L)
    expectMsg(s"0:$etlName:0")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 0, 0)
    expectMsg(s"0:$etlName:1")
    Constants.offsetCheckerPool(Constants.multiTopic1).offsets = Map(0 -> 5L)

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 0, 0)
    expectMsg(s"5:$etlName:2")
    Constants.offsetCheckerPool(Constants.multiTopic1).offsets = Map(0 -> 5L)

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 0, 3)
    expectMsg(s"2:$etlName:3")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 3, 5)
    expectMsg(s"0:$etlName:4")

    Constants.offsetCheckerPool(Constants.multiTopic1).offsets = Map(0 -> 1000L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 5, 5)
    expectMsg(s"995:$etlName:5")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 5, 500)
    expectMsg(s"500:$etlName:6")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic1, 5, 1000)
    expectMsg(s"0:$etlName:7")

    // TOPIC 2
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 0, 0)
    while (!Constants.offsetCheckerPool.contains(Constants.multiTopic2)) {}
    Constants.offsetCheckerPool(Constants.multiTopic2).offsets = Map(0 -> 0L)
    expectMsg(s"0:$etlName:0")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 0, 0)
    expectMsg(s"0:$etlName:1")

    Constants.offsetCheckerPool(Constants.multiTopic2).offsets = Map(0 -> 5L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 0, 0)
    expectMsg(s"5:$etlName:2")

    Constants.offsetCheckerPool(Constants.multiTopic2).offsets = Map(0 -> 6L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 0, 3)
    expectMsg(s"3:$etlName:3")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 3, 5)
    expectMsg(s"1:$etlName:4")

    Constants.offsetCheckerPool(Constants.multiTopic2).offsets = Map(0 -> 1000L)
    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 5, 6)
    expectMsg(s"994:$etlName:5")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 5, 501)
    expectMsg(s"499:$etlName:6")

    backlogGuardian ! telemetryMessageSourcesSummary(Constants.multiTopic2, 5, 1000)
    expectMsg(s"0:$etlName:7")

    backlogGuardian ! Stop
    expectMsg(Right(()))
    expectNoMsg()
  }

  def telemetryMessageSourcesSummary(topicName: String, startOffset: Long, endOffset: Long) =
    TelemetryMessageSourcesSummary(
      Seq(
        TelemetryMessageSource(
          messageId = UUID.randomUUID().toString,
          sourceId = Constants.sourceIds(topicName),
          timestamp = new java.util.Date().toString,
          description = "description",
          startOffset = Map(topicName -> Map("0" -> startOffset)),
          endOffset = Map(topicName   -> Map("0" -> endOffset))
        )
      )
    )
}
