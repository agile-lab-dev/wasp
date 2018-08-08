package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit, TestProbe}
import it.agilelab.bigdata.wasp.core.models.{PipegraphInstanceModel, PipegraphStatus, _}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => ChildProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.{MockPipegraphBl, MockPipegraphInstanceBl, pipegraph}


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
        PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, 0l, PipegraphStatus.PROCESSING),
        PipegraphInstanceModel("pipegraph-3", "pipegraph", 1l, 0l, PipegraphStatus.STOPPING)

      )

      startingInstances.foreach(mockBl.instances().insert(_))


      val probe = TestProbe()

      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator,
        1.millisecond))


      fsm.stateName should be(Initialized)

      mockBl.instances().all() should matchPattern {
        case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, _, PipegraphStatus.PENDING, None),
        PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, _, PipegraphStatus.PENDING, None),
        PipegraphInstanceModel("pipegraph-3", "pipegraph", 1l, _, PipegraphStatus.STOPPED, None)) =>
      }


      probe.expectMsg(WorkAvailable)
      probe.expectMsg(WorkAvailable)

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
        PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, 0l, PipegraphStatus.PROCESSING)
      )

      startingInstances.foreach(mockBl.instances().insert(_))


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))


      eventually(timeout(Span(10, Seconds))) {

        fsm.stateName should be(Initialized)

        mockBl.instances().all() should matchPattern {
          case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, _, PipegraphStatus.PENDING, None),
          PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, _, PipegraphStatus.PENDING, None)) =>
        }


        probe.expectMsg(WorkAvailable)
        probe.expectMsg(WorkAvailable)
        Unit
      }
    }


    "Allow direct stop of PENDING pipegraphs" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-b", 1l, 0l, PipegraphStatus.PENDING)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      mockBl.insert(PipegraphModel(name = "pipegraph-a",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None))

      mockBl.insert(PipegraphModel(name = "pipegraph-b",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None))


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref
      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)
      probe.expectMsg(WorkAvailable)

      fsm ! StopPipegraph("pipegraph-a")

      expectMsg(PipegraphStopped("pipegraph-a"))
    }


    "Inform worker of Stopping pipegraph" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph-b", 1l, 0l, PipegraphStatus.PENDING)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      mockBl.insert(PipegraphModel(name = "pipegraph-a",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None))

      mockBl.insert(PipegraphModel(name = "pipegraph-b",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None))


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)
      probe.expectMsg(WorkAvailable)

      probe.reply(ChildProtocol.GimmeWork)

      probe.expectMsgType[WorkGiven]


      fsm ! StopPipegraph("pipegraph-a")

      expectMsg(PipegraphStopped("pipegraph-a"))

      probe.expectMsg(ChildProtocol.CancelWork)

      mockBl.instances().instancesOf("pipegraph-a") should matchPattern {
        case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1l, _, PipegraphStatus.STOPPING, None)) =>
      }

      probe.reply(ChildProtocol.WorkCancelled)

      eventually(timeout(Span(10, Seconds))) {
        mockBl.instances().instancesOf("pipegraph-a") should matchPattern {
          case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1l, _, PipegraphStatus.STOPPED, None)) =>
        }
      }

    }

    "Retry stop of failed stoppings" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)


      mockBl.instances().insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING))

      mockBl.insert(PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None))


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)
      probe.reply(pipegraph.Protocol.GimmeWork)
      probe.expectMsgType[WorkGiven]

      fsm ! StopPipegraph("pipegraph")

      probe.expectMsg(pipegraph.Protocol.CancelWork)

      probe.reply(ChildProtocol.WorkNotCancelled(new Exception("Sorry cannot cancel")))

      probe.expectMsg(ChildProtocol.CancelWork)

      probe.reply(ChildProtocol.WorkCancelled)

      expectMsgType[PipegraphStopped]
    }

    "Prevent Start of already PENDING or PROCESSING pipegraphs" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, 0l, PipegraphStatus.PENDING)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      mockBl.insert(PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None))


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)
      probe.expectMsg(WorkAvailable)

      fsm ! StartPipegraph("pipegraph")

      expectMsg(PipegraphNotStarted("pipegraph", "Cannot start more than one instance of [pipegraph]"))

    }

    "Give work to childrens that ask for it" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, 0l, PipegraphStatus.PENDING)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      val pipegraph = PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None)

      mockBl.insert(pipegraph)


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref
      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref

      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)
      probe.expectMsg(WorkAvailable)


      fsm ! ChildProtocol.GimmeWork

      val work = expectMsgType[WorkGiven]


      work should matchPattern {
        case WorkGiven(`pipegraph`, PipegraphInstanceModel(_, pipegraph.name, 1l, _, PipegraphStatus.PROCESSING, None)) =>
      }


    }


    "Signal error to children if work cannot be given" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl) {

        override def getByName(name: String): Option[PipegraphModel] = throw new Exception("Service is temporary down")

      }

      val startingInstances = Seq(
        PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING),
        PipegraphInstanceModel("pipegraph-2", "pipegraph", 1l, 0l, PipegraphStatus.PENDING)
      )

      startingInstances.foreach(mockBl.instances().insert(_))

      val pipegraph = PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None)

      mockBl.insert(pipegraph)


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)
      probe.expectMsg(WorkAvailable)


      fsm ! ChildProtocol.GimmeWork

      expectMsgType[WorkNotGiven].reason.getMessage should be("Service is temporary down")


    }


    "Mark failed pipegraph and add exception to database" in {


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)


      mockBl.instances().insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING))

      val pipegraph = PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None)

      mockBl.insert(pipegraph)


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)

      probe.reply(ChildProtocol.GimmeWork)

      probe.expectMsgType[WorkGiven]

      val exception = new Exception("Work failed")

      probe.reply(WorkFailed(exception))

      val expected = ExceptionUtils.getStackTrace(exception)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, _, PipegraphStatus.FAILED, Some(`expected`))) =>
        }
      }


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


      mockBl.instances().insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING))

      val pipegraph = PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None)

      mockBl.insert(pipegraph)


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)

      probe.reply(ChildProtocol.GimmeWork)

      probe.expectMsgType[WorkGiven]

      val exception = new Exception("Work failed")

      probe.reply(WorkFailed(exception))

      val expected = ExceptionUtils.getStackTrace(exception)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, _, PipegraphStatus.FAILED, Some(`expected`))) =>
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


      mockBl.instances().insert(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, 0l, PipegraphStatus.PENDING))

      val pipegraph = PipegraphModel(name = "pipegraph",
        description = "",
        owner = "test",
        isSystem = false,
        creationTime = System.currentTimeMillis(),
        legacyStreamingComponents = List.empty,
        structuredStreamingComponents = List.empty,
        rtComponents = List.empty,
        dashboard = None)

      mockBl.insert(pipegraph)


      val probe = TestProbe()
      val childCreator: ChildCreator = (_,_,_) => probe.ref

      val watchdogCreator: ChildCreator = (master,name, _) => TestProbe(name)(system).ref
      val fsm = TestFSMRef(new SparkConsumersStreamingMasterGuardian(mockBl, childCreator,watchdogCreator, 1.millis))

      probe.expectMsg(WorkAvailable)

      probe.reply(ChildProtocol.GimmeWork)

      probe.expectMsgType[WorkGiven]


      probe.reply(WorkCompleted)


      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(PipegraphInstanceModel("pipegraph-1", "pipegraph", 1l, _, PipegraphStatus.STOPPED, None)) =>
        }
      }


    }

  }


}
