package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorSystem, Props}
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.collaborator.CollaboratorActor
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => ChildProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.{pipegraph, MockPipegraphBl, MockPipegraphInstanceBl}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}

class SparkConsumersStreamingMasterGuardianSpec
    extends TestKit(ActorSystem("WASP"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with Matchers
    with Eventually {

  import SparkConsumersStreamingMasterGuardian._

  import State._
  import Data._
  import Protocol._

  import scala.concurrent.duration._

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A SparkConsumersStreamingMasterGuardian" must {

    "Reset all PROCESSING pipegraphs to PENDING and STOPPING to STOPPED when starting and start children actors" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl())

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-1", 1L, 0L, PipegraphStatus.PENDING, None, None),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-2", 1L, 0L, PipegraphStatus.PROCESSING, Some("node"), None),
        PipegraphInstanceModel("pipegraph-3", "pipegraph", 1L, 0L, PipegraphStatus.STOPPING, Some("node"), None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()

      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a1", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a1")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      val models = mockBl.instances().all()

      models should matchPattern {
        case Seq(
            PipegraphInstanceModel("pipegraph-1", "pipegraph-1", 1L, _, PipegraphStatus.PENDING, None, None, None),
            PipegraphInstanceModel("pipegraph-2", "pipegraph-2", 1L, _, PipegraphStatus.PENDING, None, None, None),
            PipegraphInstanceModel("pipegraph-3", "pipegraph", 1L, _, PipegraphStatus.STOPPED, Some("node"), None, None)
            ) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph-1"))
      probe.expectMsg(WorkAvailable("pipegraph-2"))

      system.stop(fsm)
      system.stop(collaborator)
    }

    "Retry initialization if something fails" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl() {

        val atomicInteger = new AtomicInteger()

        override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel = {

          val attempt = atomicInteger.incrementAndGet()

          if (attempt < 6) {
            throw new Exception("Service is temporary down")
          }

          super.update(instance)
        }

      })

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-1", 1L, 0L, PipegraphStatus.PENDING, None, None),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-2", 1L, 0L, PipegraphStatus.PROCESSING, Some("node"), None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref
      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a2", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a2")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all() should matchPattern {
          case Seq(
              PipegraphInstanceModel("pipegraph-1", "pipegraph-1", 1L, _, PipegraphStatus.PENDING, None, None, None),
              PipegraphInstanceModel("pipegraph-2", "pipegraph-2", 1L, _, PipegraphStatus.PENDING, None, None, None)
              ) =>
        }

        probe.expectMsg(WorkAvailable("pipegraph-1"))
        probe.expectMsg(WorkAvailable("pipegraph-2"))

        system.stop(fsm)
        system.stop(collaborator)
        Unit
      }
    }

    "Allow direct stop of PENDING pipegraphs" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-a", "pipegraph-a", 1L, 0L, PipegraphStatus.PENDING, None, None),
        PipegraphInstanceModel("pipegraph-b", "pipegraph-b", 1L, 0L, PipegraphStatus.PENDING, None, None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph-a",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph-b",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      val probe                         = TestProbe()
      val childCreator: ChildCreator    = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a3", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a3")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph-a"))
      probe.expectMsg(WorkAvailable("pipegraph-b"))

      fsm ! StopPipegraph("pipegraph-a")

      expectMsg(PipegraphStopped("pipegraph-a"))

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Inform worker of Stopping pipegraph" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-a", "pipegraph-a", 1L, 0L, PipegraphStatus.PENDING, None, None),
        PipegraphInstanceModel("pipegraph-b", "pipegraph-b", 1L, 0L, PipegraphStatus.PENDING, None, None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph-a",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph-b",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a4", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a4")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph-a"))
      probe.expectMsg(WorkAvailable("pipegraph-b"))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress))

      probe.expectMsgType[WorkGiven]

      fsm ! StopPipegraph("pipegraph-a")

      expectMsg(PipegraphStopped("pipegraph-a"))

      probe.expectMsg(ChildProtocol.CancelWork)

      val instances = mockBl.instances().instancesOf("pipegraph-a")

      instances.foreach(println)

      val uniqueAddress = SparkConsumersStreamingMasterGuardian.formatUniqueAddress(Cluster(system).selfUniqueAddress)
      val probeAddr     = probe.ref.path.toString

      instances.head.shouldBe(
        PipegraphInstanceModel(
          "pipegraph-a",
          "pipegraph-a",
          1L,
          instances.head.currentStatusTimestamp,
          PipegraphStatus.STOPPING,
          Some(uniqueAddress),
          Some(probeAddr),
          None
        )
      )

      probe.reply(ChildProtocol.WorkCancelled)

      eventually(timeout(Span(10, Seconds))) {
        mockBl.instances().instancesOf("pipegraph-a") should matchPattern {
          case Seq(
              PipegraphInstanceModel(
                "pipegraph-a",
                "pipegraph-a",
                1L,
                _,
                PipegraphStatus.STOPPED,
                None,
                None,
                None
              )
              ) =>
        }
      }

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Retry stop of failed stoppings" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      mockBl
        .instances()
        .insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1L, 0L, PipegraphStatus.PENDING, None, None, None))

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a5", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a5")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph"))
      probe.reply(pipegraph.Protocol.GimmeWork(Cluster(system).selfUniqueAddress))
      probe.expectMsgType[WorkGiven]

      fsm ! StopPipegraph("pipegraph")

      probe.expectMsg(pipegraph.Protocol.CancelWork)

      probe.reply(ChildProtocol.WorkNotCancelled(new Exception("Sorry cannot cancel")))

      probe.expectMsg(ChildProtocol.CancelWork)

      probe.reply(ChildProtocol.WorkCancelled)

      expectMsgType[PipegraphStopped]

      system.stop(fsm)
      system.stop(collaborator)
    }

    "Prevent Start of already PENDING or PROCESSING pipegraphs" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-1", 1L, 0L, PipegraphStatus.PENDING, None, None, None),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-2", 1L, 0L, PipegraphStatus.PENDING, None, None, None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph-1",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      mockBl.insert(
        PipegraphModel(
          name = "pipegraph-2",
          description = "",
          owner = "test",
          isSystem = false,
          creationTime = System.currentTimeMillis(),
          legacyStreamingComponents = List.empty,
          structuredStreamingComponents = List.empty,
          rtComponents = List.empty,
          dashboard = None
        )
      )

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a6", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a6")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph-1"))
      probe.expectMsg(WorkAvailable("pipegraph-2"))

      fsm ! StartPipegraph("pipegraph-1")

      expectMsg(PipegraphNotStarted("pipegraph-1", "Cannot start more than one instance of [pipegraph-1]"))

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Give work to childrens that ask for it" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1L, 0L, PipegraphStatus.PENDING, None, None),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-b", 1L, 0L, PipegraphStatus.PENDING, None, None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      val pipegraph = PipegraphModel(
        name = "pipegraph-a",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None
      )

      val pipegraph2 = pipegraph.copy(name = "pipegraph-b")

      mockBl.insert(pipegraph)
      mockBl.insert(pipegraph2)

      val probe                         = TestProbe()
      val childCreator: ChildCreator    = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a7", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a7")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph-a"))
      probe.expectMsg(WorkAvailable("pipegraph-b"))

      fsm ! ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress)
      fsm ! ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress)

      val work = expectMsgType[WorkGiven]

      work should matchPattern {
        case WorkGiven(
            `pipegraph`,
            PipegraphInstanceModel(_, pipegraph.name, 1L, _, PipegraphStatus.PROCESSING, _, _, None)
            ) =>
      }

      val work2 = expectMsgType[WorkGiven]
      work2 should matchPattern {
        case WorkGiven(
            `pipegraph2`,
            PipegraphInstanceModel(_, pipegraph2.name, 1L, _, PipegraphStatus.PROCESSING, _, _, None)
            ) =>
      }

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Signal error to children if work cannot be given" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl) {

        override def getByName(name: String): Option[PipegraphModel] = throw new Exception("Service is temporary down")

      }

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1L, 0L, PipegraphStatus.PENDING, None, None),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-b", 1L, 0L, PipegraphStatus.PENDING, None, None)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      val pipegraph = PipegraphModel(
        name = "pipegraph-a",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None
      )

      mockBl.insert(pipegraph)
      mockBl.insert(pipegraph.copy(name = "pipegraph-b"))

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a8", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a8")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph-a"))
      probe.expectMsg(WorkAvailable("pipegraph-b"))

      fsm ! ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress)

      expectMsgType[WorkNotGiven].reason.getMessage should be("Service is temporary down")

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Mark failed pipegraph and add exception to database" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      mockBl
        .instances()
        .insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1L, 0L, PipegraphStatus.PENDING, None, None))

      val pipegraph = PipegraphModel(
        name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None
      )

      mockBl.insert(pipegraph)

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a9", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a9")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph"))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress))

      probe.expectMsgType[WorkGiven]

      val exception = new Exception("Work failed")

      probe.reply(WorkFailed(exception))

      val expected = ExceptionUtils.getStackTrace(exception)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(
              PipegraphInstanceModel(
                "pipegraph-1",
                "pipegraph",
                1L,
                _,
                PipegraphStatus.FAILED,
                _,
                _,
                Some(`expected`)
              )
              ) =>
        }
      }

      system.stop(fsm)
      system.stop(collaborator)
    }

    "Retry database access to update failed pipegraph" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl {

        val callCount = new AtomicInteger()

        override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel =
          if (instance.status == PipegraphStatus.FAILED && callCount.incrementAndGet() == 1) {
            throw new Exception("Service is temporary down")
          } else {
            super.update(instance)
          }

      })

      mockBl
        .instances()
        .insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1L, 0L, PipegraphStatus.PENDING, None, None))

      val pipegraph = PipegraphModel(
        name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None
      )

      mockBl.insert(pipegraph)

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a10", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a10")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph"))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress))

      probe.expectMsgType[WorkGiven]

      val exception = new Exception("Work failed")

      probe.reply(WorkFailed(exception))

      val expected = ExceptionUtils.getStackTrace(exception)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(
              PipegraphInstanceModel(
                "pipegraph-1",
                "pipegraph",
                1L,
                _,
                PipegraphStatus.FAILED,
                None,
                None,
                Some(`expected`)
              )
              ) =>
        }
      }

    }

    "Retry database access to update completed pipegraph" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl {

        val callCount = new AtomicInteger()

        override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel =
          if (instance.status == PipegraphStatus.STOPPED && callCount.incrementAndGet() == 1) {
            throw new Exception("Service is temporary down")
          } else {
            super.update(instance)
          }

      })

      mockBl
        .instances()
        .insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1L, 0L, PipegraphStatus.PENDING, None, None))

      val pipegraph = PipegraphModel(
        name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None
      )

      mockBl.insert(pipegraph)

      val probe                      = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref
      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-11", 1.millisecond)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-11")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle)                    =>
        case Transition(_, Idle, Initializing)        =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable("pipegraph"))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress))

      probe.expectMsgType[WorkGiven]

      probe.reply(WorkCompleted)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(
              PipegraphInstanceModel("pipegraph-1", "pipegraph", 1L, _, PipegraphStatus.STOPPED, None, None, None)
              ) =>
        }
      }

    }

  }

}
