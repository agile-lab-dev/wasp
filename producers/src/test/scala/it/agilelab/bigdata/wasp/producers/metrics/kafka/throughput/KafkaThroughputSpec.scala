package it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.bigdata.wasp.core.messages.{Start, Stop}
import it.agilelab.bigdata.wasp.producers.StartMainTask
import it.agilelab.bigdata.wasp.producers.metrics.kafka.Constants
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration.FiniteDuration

class KafkaThroughputSpec extends TestKit(
  ActorSystem("BacklogSizeAnalyzerSpec",
    ConfigFactory.load()
      .withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0)))
) with ImplicitSender with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  it should "correctly start and stop all the actors and output a correct throughput through epocs" in {
    val throughputGuardian = system.actorOf(Props(new TestKafkaThroughputGuardian), "TestKafkaThroughputGuardian")
    throughputGuardian ! Start
    throughputGuardian ! StartMainTask
    Constants.testThroughputActor = testActor
    expectMsg(FiniteDuration(20, TimeUnit.SECONDS), Right(()))
    while (!Constants.offsetCheckerPool.contains(Constants.throughputTestTopic)) {}
    Constants.offsetCheckerPool(Constants.throughputTestTopic).offsets = Map(0 -> 0L)
    for {i <- 0 until 10} expectMsg(s"0:$i")
    Constants.offsetCheckerPool(Constants.throughputTestTopic).offsets = Map(0 -> 10L)
    expectMsg("10:10")
    Constants.offsetCheckerPool(Constants.throughputTestTopic).offsets = Map(0 -> 20L)
    expectMsg("20:11")
    Constants.offsetCheckerPool(Constants.throughputTestTopic).offsets = Map(0 -> 30L)
    expectMsg("30:12")
    Constants.offsetCheckerPool(Constants.throughputTestTopic).offsets = Map(0 -> 35L)
    expectMsg("35:13")
    for {i <- 14 until 20} expectMsg(s"35:$i")
    expectMsg("25:20")
    expectMsg("15:21")
    expectMsg("5:22")
    expectMsg("0:23")
    expectMsg("0:24")
    throughputGuardian ! Stop
    expectNoMsg()
  }
}