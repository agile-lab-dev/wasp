package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.bigdata.wasp.core.messages.{Start, Stop, TelemetryMessageSource, TelemetryMessageSourcesSummary}
import it.agilelab.bigdata.wasp.producers.metrics.kafka.Constants
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class BacklogSizeAnalyzerSpec extends TestKit(
  ActorSystem("BacklogSizeAnalyzerSpec",
    ConfigFactory.load()
      .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0))
  )
)
  with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  it should "start, calculate backlog and then stop gracefully" in {
    val backlogGuardian = system.actorOf(Props(new TestBacklogSizeAnalyzerProducerGuardian(testActor)), "TestBacklogSizeAnalyzerProducerGuardian")
    backlogGuardian ! Start
    expectMsg(Right(()))
    backlogGuardian ! telemetryMessageSourcesSummary(0, 0)
    while (!Constants.offsetCheckerPool.contains(Constants.backlogTestTopic)) {}
    Constants.offsetCheckerPool(Constants.backlogTestTopic).offsets = Map(0 -> 0L)
    expectMsg("0:testEtl:0")
    backlogGuardian ! telemetryMessageSourcesSummary(0, 0)
    expectMsg("0:testEtl:1")
    Constants.offsetCheckerPool(Constants.backlogTestTopic).offsets = Map(0 -> 5L)
    backlogGuardian ! telemetryMessageSourcesSummary(0, 0)
    expectMsg("5:testEtl:2")
    Constants.offsetCheckerPool(Constants.backlogTestTopic).offsets = Map(0 -> 5L)
    backlogGuardian ! telemetryMessageSourcesSummary(0, 3)
    expectMsg("2:testEtl:3")
    backlogGuardian ! telemetryMessageSourcesSummary(3, 5)
    expectMsg("0:testEtl:4")
    Constants.offsetCheckerPool(Constants.backlogTestTopic).offsets = Map(0 -> 1000L)
    backlogGuardian ! telemetryMessageSourcesSummary(5, 5)
    expectMsg("995:testEtl:5")
    backlogGuardian ! telemetryMessageSourcesSummary(5, 500)
    expectMsg("500:testEtl:6")
    backlogGuardian ! telemetryMessageSourcesSummary(5, 1000)
    expectMsg("0:testEtl:7")
    backlogGuardian ! Stop
    expectMsg(Right(()))
    expectNoMsg()
  }

  def telemetryMessageSourcesSummary(startOffset: Long, endOffset: Long) = TelemetryMessageSourcesSummary(
    Seq(TelemetryMessageSource(
      messageId = UUID.randomUUID().toString,
      sourceId = Constants.sourceId,
      timestamp = new java.util.Date().toString,
      description = "description",
      startOffset = Map(Constants.backlogTestTopic -> Map("0" -> startOffset)),
      endOffset = Map(Constants.backlogTestTopic -> Map("0" -> endOffset))
    ))
  )
}
