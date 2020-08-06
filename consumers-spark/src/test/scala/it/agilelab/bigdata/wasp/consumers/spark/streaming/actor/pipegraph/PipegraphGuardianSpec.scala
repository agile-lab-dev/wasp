package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph


import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{Protocol => ETLProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.{Protocol => MasterProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.Data.{MaterializingData, WorkerToEtlAssociation}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.State._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => PipegraphProtocol}
import it.agilelab.bigdata.wasp._
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.collection.immutable.Map
import scala.concurrent.duration._


class ProbesFactory(implicit val actorSystem: ActorSystem) extends ((PipegraphModel,String, ActorRefFactory) =>
  ActorRef) {

  var probes: Seq[TestProbe] = Seq.empty

  override def apply(pipegraphModel:PipegraphModel,name: String, factory: ActorRefFactory): ActorRef = {
    val probe = TestProbe()
    probes = probes :+ probe
    probe.ref
  }

}


class PipegraphGuardianSpec extends TestKit(ActorSystem("WASP"))
  with WordSpecLike
  with BeforeAndAfterAll
  with ImplicitSender
  with Matchers
  with Eventually
  with Inside {

  val defaultPipegraph = PipegraphModel(name = "pipegraph",
    description = "",
    owner = "test",
    isSystem = false,
    creationTime = System.currentTimeMillis(),
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(name = "component",
                                  streamingInput = StreamingReaderModel.kafkaReader("", DatastoreModelsForTesting.TopicModels.json, None),
                                  staticInputs = List.empty,
                                  streamingOutput = WriterModel.solrWriter("", DatastoreModelsForTesting.IndexModels.solr),
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
    status = PipegraphStatus.PROCESSING,
    executedByNode = Some("node"),peerActor = None)

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A PipegraphGuardian is in WaitingForWorkState" must {

    "Ask for work when work is available" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref, "pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
    }

    "Retry if work cannot be given" in {


      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref, "pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkNotGiven(new Exception("Something went wrong")))

      transitions.expectMsg(Transition[State](fsm, RequestingWork, RequestingWork))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkNotGiven(new Exception("Something went wrong")))

      transitions.expectMsg(Transition[State](fsm, RequestingWork, RequestingWork))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(defaultPipegraph.structuredStreamingComponents.head))

      factory.probes.head.reply(ETLProtocol.ETLActivated(defaultPipegraph.structuredStreamingComponents.head))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

    }
  }


  "A Pipegraph in Activating State" must {

    "Activate ETL component" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(defaultPipegraph.structuredStreamingComponents.head))

      factory.probes.head.reply(ETLProtocol.ETLActivated(defaultPipegraph.structuredStreamingComponents.head))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))


    }


    "Honor dont care strategy" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref, "pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(defaultPipegraph.structuredStreamingComponents.head))

      factory.probes.head.reply(ETLProtocol.ETLNotActivated(defaultPipegraph.structuredStreamingComponents.head, new
          Exception("Error!")))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))


      fsm.stateData should be(MaterializingData(defaultPipegraph, defaultInstance))

    }


    "Honor Retry strategy" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => Retry

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref, "pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLNotActivated(etl, new
          Exception("Error!")))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes(1).expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes(1).reply(ETLProtocol.ETLActivated(etl))



      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      val expected = MaterializingData(defaultPipegraph,
                                       defaultInstance,
                                       Set.empty,
                                       Set(WorkerToEtlAssociation(factory.probes(1).ref, etl)))

      fsm.stateData should be(expected)

    }


    "Honor StopALL strategy" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => StopAll

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      val reason = new Exception("Error!")

      factory.probes.head.reply(ETLProtocol.ETLNotActivated(etl, reason))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Stopping))

      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))

      master.expectMsg(MasterProtocol.WorkFailed(reason))

    }

    "Honor StopALL strategy Multiple etlcomponent" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val failingEtl = defaultPipegraph.structuredStreamingComponents.head.copy(name="failing-component")
      val etl = defaultPipegraph.structuredStreamingComponents.head
      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(etl,failingEtl))

      val strategy: ComponentFailedStrategy = {
        case `failingEtl` => StopAll
        case `etl` => DontCare
      }

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))




      master.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))



      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val reason = new Exception("Ops!")

      factory.probes(1).expectMsg(ETLProtocol.ActivateETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLNotActivated(failingEtl, reason))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))
      transitions.expectMsg(Transition[State](fsm, Activated, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopping))

      factory.probes.head.expectMsg(ETLProtocol.StopETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLStopped(etl))

      transitions.expectMsg(Transition[State](fsm, Stopping, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))


    }


  }

  "A PipegraphGuardian in Materializing State" must {

    "Materialize ETLs" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable("pipegraph"))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
    }


    "Honor dont care strategy" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      val reason = new Exception("Error!")

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLNotMaterialized(etl,reason))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))


    }

    "Honor retry strategy" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => Retry

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))

      val reason = new Exception("Error!")

      factory.probes.head.reply(ETLProtocol.ETLNotMaterialized(etl, reason))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
    }


    "Honor StopAll strategy" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => StopAll

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      val reason = new Exception("Error!")

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLNotMaterialized(etl, reason))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))

      master.expectMsg(MasterProtocol.WorkFailed(reason))
    }

    "Honor StopALL strategy Multiple etlcomponent" in {

      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val failingEtl = defaultPipegraph.structuredStreamingComponents.head.copy(name="failing-component")
      val etl = defaultPipegraph.structuredStreamingComponents.head
      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(etl,failingEtl))

      val strategy: ComponentFailedStrategy = {
        case `failingEtl` => StopAll
        case `etl` => DontCare
      }

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))




      master.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))



      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes(1).expectMsg(ETLProtocol.ActivateETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLActivated(failingEtl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      val reason = new Exception("Ops!")

      factory.probes(1).expectMsg(ETLProtocol.MaterializeETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLNotMaterialized(failingEtl, reason))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopping))

      factory.probes.head.expectMsg(ETLProtocol.StopETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLStopped(etl))

      transitions.expectMsg(Transition[State](fsm, Stopping, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))

    }
  }


  "A PipegraphGuardian in Monitoring State" must {

    "Monitor ETLs" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(etl))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))

      transitions.expectMsg(Transition[State](fsm, Monitored, Monitoring))

      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(etl))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))
    }




    "Honor dont care strategy" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => DontCare

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLCheckFailed(etl, new Exception("Ops!")))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))

      transitions.expectMsg(Transition[State](fsm, Monitored, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))

      master.expectMsg(MasterProtocol.WorkCompleted)
    }


    "Honor stop all strategy" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()


      val failingEtl = defaultPipegraph.structuredStreamingComponents.head.copy(name="failing-component")
      val etl = defaultPipegraph.structuredStreamingComponents.head
      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(etl,failingEtl))

      val strategy: ComponentFailedStrategy = {
        case `failingEtl` => StopAll
        case `etl` => DontCare
      }

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system  ).selfUniqueAddress, "pipegraph"))




      master.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))



      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes(1).expectMsg(ETLProtocol.ActivateETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLActivated(failingEtl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      factory.probes(1).expectMsg(ETLProtocol.MaterializeETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLMaterialized(failingEtl))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))


      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))

      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(etl))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      factory.probes(1).expectMsg(ETLProtocol.CheckETL(failingEtl))

      val reason = new Exception("Ops!")

      factory.probes(1).reply(ETLProtocol.ETLCheckFailed(failingEtl, reason))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))


      transitions.expectMsg(Transition[State](fsm, Monitored, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopping))

      factory.probes.head.expectMsg(ETLProtocol.StopETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLStopped(etl))

      transitions.expectMsg(Transition[State](fsm, Stopping, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))



    }

    "Be Reactive to stop request" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()

      val strategy: ComponentFailedStrategy = _ => StopAll

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))

      master.send(fsm, MasterProtocol.WorkGiven(defaultPipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      val etl = defaultPipegraph.structuredStreamingComponents.head

      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))

      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))
      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(PipegraphProtocol.CancelWork)

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))
      transitions.expectMsg(Transition[State](fsm, Monitored, Stopping))
      transitions.expectMsg(Transition[State](fsm, Stopping, Stopped))
    }


    "Honor Retry strategy" in {
      val master = TestProbe()
      val transitions = TestProbe()
      val factory = new ProbesFactory()



      val failingEtl = defaultPipegraph.structuredStreamingComponents.head.copy(name="failing-component")
      val etl = defaultPipegraph.structuredStreamingComponents.head
      val failingEtl2 = defaultPipegraph.structuredStreamingComponents.head.copy(name="etl2")

      val pipegraph = defaultPipegraph.copy(structuredStreamingComponents = List(etl,failingEtl,failingEtl2))

      val strategy: ComponentFailedStrategy = {
        case `failingEtl` => Retry
        case `failingEtl2` => Retry
        case `etl` => DontCare
      }

      val fsm = TestFSMRef(new PipegraphGuardian(master.ref,"pipegraph", factory, 500.milliseconds, 500.milliseconds, strategy))

      transitions.send(fsm, SubscribeTransitionCallBack(transitions.ref))
      transitions.expectMsgType[CurrentState[State]]

      master.send(fsm, MasterProtocol.WorkAvailable(defaultPipegraph.name))

      master.expectMsg(PipegraphProtocol.GimmeWork(Cluster(system).selfUniqueAddress, "pipegraph"))




      master.send(fsm, MasterProtocol.WorkGiven(pipegraph, defaultInstance))

      transitions.expectMsg(Transition[State](fsm, WaitingForWork, RequestingWork))
      transitions.expectMsg(Transition[State](fsm, RequestingWork, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))



      factory.probes.head.expectMsg(ETLProtocol.ActivateETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLActivated(etl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      factory.probes(1).expectMsg(ETLProtocol.ActivateETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLActivated(failingEtl))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))


      factory.probes(2).expectMsg(ETLProtocol.ActivateETL(failingEtl2))
      factory.probes(2).reply(ETLProtocol.ETLActivated(failingEtl2))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))


      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes.head.expectMsg(ETLProtocol.MaterializeETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLMaterialized(etl))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      factory.probes(1).expectMsg(ETLProtocol.MaterializeETL(failingEtl))
      factory.probes(1).reply(ETLProtocol.ETLMaterialized(failingEtl))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))


      factory.probes(2).expectMsg(ETLProtocol.MaterializeETL(failingEtl2))
      factory.probes(2).reply(ETLProtocol.ETLMaterialized(failingEtl2))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))

      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))



      factory.probes(1).expectMsg(ETLProtocol.CheckETL(failingEtl))

      val reason = new Exception("Ops!")

      factory.probes(1).reply(ETLProtocol.ETLCheckFailed(failingEtl, reason))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(etl))

      factory.probes(2).expectMsg(ETLProtocol.CheckETL(failingEtl2))
      factory.probes(2).reply(ETLProtocol.ETLCheckFailed(failingEtl2, reason))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))
      transitions.expectMsg(Transition[State](fsm, Monitored, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))





      factory.probes(3).expectMsg(ETLProtocol.ActivateETL(failingEtl))
      factory.probes(3).reply(ETLProtocol.ETLActivated(failingEtl))
      factory.probes(4).expectMsg(ETLProtocol.ActivateETL(failingEtl2))
      factory.probes(4).reply(ETLProtocol.ETLActivated(failingEtl2))

      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))
      transitions.expectMsg(Transition[State](fsm, Activating, Activating))

      transitions.expectMsg(Transition[State](fsm, Activating, Activated))

      transitions.expectMsg(Transition[State](fsm, Activated, Materializing))

      factory.probes(3).expectMsg(ETLProtocol.MaterializeETL(failingEtl))
      factory.probes(3).reply(ETLProtocol.ETLMaterialized(failingEtl))
      factory.probes(4).expectMsg(ETLProtocol.MaterializeETL(failingEtl2))
      factory.probes(4).reply(ETLProtocol.ETLMaterialized(failingEtl2))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))
      transitions.expectMsg(Transition[State](fsm, Materializing, Materializing))

      transitions.expectMsg(Transition[State](fsm, Materializing, Materialized))

      transitions.expectMsg(Transition[State](fsm, Materialized, Monitoring))


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes.head.reply(ETLProtocol.ETLCheckSucceeded(etl))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      factory.probes(3).expectMsg(ETLProtocol.CheckETL(failingEtl))
      factory.probes(3).reply(ETLProtocol.ETLCheckSucceeded(failingEtl))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      factory.probes(4).expectMsg(ETLProtocol.CheckETL(failingEtl2))
      factory.probes(4).reply(ETLProtocol.ETLCheckSucceeded(failingEtl2))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))
      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitoring))

      transitions.expectMsg(Transition[State](fsm, Monitoring, Monitored))

      factory.probes.length should be(5)


      factory.probes.head.expectMsg(ETLProtocol.CheckETL(etl))
      factory.probes(3).expectMsg(ETLProtocol.CheckETL(failingEtl))
      factory.probes(4).expectMsg(ETLProtocol.CheckETL(failingEtl2))


    }
  }
}