package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import it.agilelab.bigdata.wasp.DatastoreModelsForTesting
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{Protocol => ETLProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.{SparkConsumersStreamingMasterGuardian, Protocol => MasterProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian.{ComponentFailedStrategy, DontCare, StopAll}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{PipegraphGuardian, ProbesFactory, Protocol => PipegraphProtocol}
import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable.Map

class IntegrationSpec
  extends TestKit(ActorSystem("WASP"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with Matchers
    with Eventually {


  import SparkConsumersStreamingMasterGuardian._

  import scala.concurrent.duration._

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val defaultPipegraph = PipegraphModel(name = "pipegraph",
    description = "",
    owner = "test",
    isSystem = false,
    creationTime = System.currentTimeMillis(),
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(name = "component",
        inputs = List(ReaderModel.kafkaReader("", DatastoreModelsForTesting.TopicModels.json)),
        output = WriterModel.solrWriter("", DatastoreModelsForTesting.IndexModels.solr),
        mlModels = List(),
        strategy = None,
        triggerIntervalMs = None,
        options = Map()
      )),
    rtComponents = List.empty,
    dashboard = None)
  val defaultInstance = PipegraphInstanceModel(name = "pipegraph-1",
    instanceOf = "pipegraph",
    startTimestamp = 1l,
    currentStatusTimestamp = 0l,
    status = PipegraphStatus.PROCESSING)

  "A SparkConsumersStreamingMasterGuardian orchestrating PipegraphGuardians" must {

    "Orchestrate one pipegraph" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl())

      mockBl.insert(defaultPipegraph)


      val probe = TestProbe()

      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare


      val childCreator: ChildCreator = (master,name, system) => system.actorOf(Props(new PipegraphGuardian(master,
        factory,
        500.milliseconds, 500.milliseconds, strategy)),name)

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref

      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator,
        1.millisecond))



      probe.send(fsm, MasterProtocol.StartPipegraph(defaultPipegraph.name))


      probe.expectMsgPF() {
        case MasterProtocol.PipegraphStarted(defaultPipegraph.name, instanceName) if instanceName.startsWith(s"${defaultPipegraph.name}-") => ()
      }

      val etl = defaultPipegraph.structuredStreamingComponents.head


      eventually(timeout(Span(10, Seconds))) {
        factory.probes.head
      }

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(etl))

      probe.send(fsm, MasterProtocol.StopPipegraph(defaultPipegraph.name))

      factory.probes.head.expectMsg(ETLProtocol.StopETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLStopped(etl))

      probe.expectMsg(MasterProtocol.PipegraphStopped(defaultPipegraph.name))

    }

    "Orchestrate more than one pipegraph" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl())


      val firstEtl = defaultPipegraph.structuredStreamingComponents.head.copy(name = "first-component")
      val secondEtl = defaultPipegraph.structuredStreamingComponents.head.copy(name = "second-component")

      val firstPipegraph = defaultPipegraph.copy(name = "first", structuredStreamingComponents = List(firstEtl))
      val secondPipegraph = defaultPipegraph.copy(name = "second", structuredStreamingComponents = List(secondEtl))


      mockBl.insert(firstPipegraph)
      mockBl.insert(secondPipegraph)


      val probe = TestProbe()

      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare


      val childCreator: ChildCreator = (master,name, system) => system.actorOf(Props(new PipegraphGuardian(master,
        factory,
        500.milliseconds, 500.milliseconds, strategy)), name)

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref

      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator,
        1.millisecond))


      probe.send(fsm, MasterProtocol.StartPipegraph(firstPipegraph.name))

      eventually(timeout(Span(10, Seconds))) {
        factory.probes.head
      }

      probe.send(fsm, MasterProtocol.StartPipegraph(secondPipegraph.name))
      probe.expectMsgPF() {
        case MasterProtocol.PipegraphStarted(firstPipegraph.name, instanceName) if instanceName.startsWith(s"${firstPipegraph.name}-") => ()
      }
      probe.expectMsgPF() {
        case MasterProtocol.PipegraphStarted(secondPipegraph.name, instanceName) if instanceName.startsWith(s"${secondPipegraph.name}-") => ()
      }

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(firstEtl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(firstEtl))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(firstEtl))

      factory.probes.head.reply(ETLProtocol.ETLMaterialized(firstEtl))

      eventually(timeout(Span(10, Seconds))) {
        factory.probes(1)
      }

      factory.probes(1).expectMsg(ETLProtocol.ActivateETL(secondEtl))

      factory.probes(1).reply(ETLProtocol.ETLActivated(secondEtl))

      factory.probes(1).expectMsg(ETLProtocol.MaterializeETL(secondEtl))

      factory.probes(1).reply(ETLProtocol.ETLMaterialized(secondEtl))

      factory.probes(1).expectMsg(ETLProtocol.CheckETL(secondEtl))

      factory.probes(1).reply(ETLProtocol.ETLCheckSucceeded(secondEtl))

      probe.send(fsm, MasterProtocol.StopPipegraph(secondPipegraph.name))

      factory.probes(1).expectMsg(ETLProtocol.StopETL(secondEtl))

      factory.probes(1).reply(ETLProtocol.ETLStopped(secondEtl))


      probe.expectMsg(MasterProtocol.PipegraphStopped(secondPipegraph.name))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(firstEtl))

      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(firstEtl))

      probe.send(fsm, MasterProtocol.StopPipegraph(firstPipegraph.name))

      factory.probes.head.expectMsg(ETLProtocol.StopETL(firstEtl))

      factory.probes.head.reply(ETLProtocol.ETLStopped(firstEtl))

      probe.expectMsg(MasterProtocol.PipegraphStopped(firstPipegraph.name))


    }

    "Record failure of pipegraph" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl())

      mockBl.insert(defaultPipegraph)


      val probe = TestProbe()

      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => StopAll


      val childCreator: ChildCreator = (master,name, system) => system.actorOf(Props(new PipegraphGuardian(master,
        factory,
        500.milliseconds, 500.milliseconds, strategy)), name)

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref

      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator, watchdogCreator,
        1.millisecond))



      probe.send(fsm, MasterProtocol.StartPipegraph(defaultPipegraph.name))


      probe.expectMsgPF() {
        case MasterProtocol.PipegraphStarted(defaultPipegraph.name, instanceName) if instanceName.startsWith(s"${defaultPipegraph.name}-") => ()
      }

      val etl = defaultPipegraph.structuredStreamingComponents.head


      eventually(timeout(Span(10, Seconds))) {
        factory.probes.head
      }

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))

      val reason = new Exception("Ops!")

      factory.probes.head.reply(ETLProtocol.ETLCheckFailed(etl, reason))

      eventually(timeout(Span(10, Seconds))) {
        mockBl.instances().all().head should matchPattern {
          case PipegraphInstanceModel(_,defaultPipegraph.name,_,_,PipegraphStatus.FAILED,Some(string)) if string ==
            getStackTrace(reason) =>
        }
      }

    }

  }
}
