package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.collaborator.CollaboratorActor
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SchedulingStrategy.SchedulingStrategyOutcome
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => ChildProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.{MockPipegraphBl, MockPipegraphInstanceBl, pipegraph}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}


trait SparkConsumersStreamingMasterGuardianSpecTestData {

  val pipegraphA = PipegraphModel(
    name = "pipegraph-a",
    description = "",
    owner = "test",
    isSystem = false,
    creationTime = System.currentTimeMillis(),
    structuredStreamingComponents = List.empty,
    dashboard = None
  )

  val pipegraphB = pipegraphA.copy(name = "pipegraph-b")
  val pipegraphC = pipegraphA.copy(name = "pipegraph-c")

  def instanceOf(pipegraph: PipegraphModel, status: PipegraphStatus.PipegraphStatus) =
    PipegraphInstanceModel(pipegraph.name + UUID.randomUUID().toString, pipegraph.name, 1L, 0L, status, None, None)

}

class SparkConsumersStreamingMasterGuardianSpec
  extends TestKit(ActorSystem("WASP"))
    with WordSpecLike
    with BeforeAndAfterAll
    with ImplicitSender
    with Matchers
    with Eventually
    with SparkConsumersStreamingMasterGuardianSpecTestData {

  import SparkConsumersStreamingMasterGuardian._

  import State._
  import Protocol._

  import scala.concurrent.duration._

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A SparkConsumersStreamingMasterGuardian" must {

    val retryInterval = 1.millisecond
    "Reset all PROCESSING pipegraphs to PENDING and STOPPING to STOPPED when starting and start children actors" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl())

      val startingPipegraphs = Seq(
        pipegraphA,
        pipegraphB,
        pipegraphC
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING),
        instanceOf(pipegraphB, PipegraphStatus.PROCESSING),
        instanceOf(pipegraphC, PipegraphStatus.STOPPING)
      )

      startingPipegraphs.foreach(mockBl.insert(_))
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()

      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a1", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a1")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      val models = mockBl.instances().all()

      models should matchPattern {
        case Seq(
        PipegraphInstanceModel(_, pipegraphA.name, 1L, _, PipegraphStatus.PENDING, None, None, None),
        PipegraphInstanceModel(_, pipegraphB.name, 1L, _, PipegraphStatus.PENDING, None, None, None),
        PipegraphInstanceModel(_, pipegraphC.name, 1L, _, PipegraphStatus.STOPPED, None, None, None)
        ) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))
      probe.expectMsg(WorkAvailable(pipegraphB.name))

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

      val startingPipegraphs = Seq(
        pipegraphA,
        pipegraphB
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING),
        instanceOf(pipegraphB, PipegraphStatus.PROCESSING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))


      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref
      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a2", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a2")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all() should matchPattern {
          case Seq(
          PipegraphInstanceModel(_, pipegraphA.name, 1L, _, PipegraphStatus.PENDING, None, None, None),
          PipegraphInstanceModel(_, pipegraphB.name, 1L, _, PipegraphStatus.PENDING, None, None, None)
          ) =>
        }

        probe.expectMsg(WorkAvailable(pipegraphA.name))
        probe.expectMsg(WorkAvailable(pipegraphB.name))

        system.stop(fsm)
        system.stop(collaborator)
        Unit
      }
    }

    "Allow direct stop of PENDING pipegraphs" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingPipegraphs = Seq(
        pipegraphA,
        pipegraphB,
        pipegraphC
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING),
        instanceOf(pipegraphB, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a3", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a3")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))
      probe.expectMsg(WorkAvailable(pipegraphB.name))

      fsm ! StopPipegraph(pipegraphA.name)

      expectMsg(PipegraphStopped(pipegraphA.name))

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Allow direct stop of UNSCHEDULABLE pipegraphs" in {

      class TestNodeLabelSchedulingStrategyFactory extends SchedulingStrategyFactory {
        override def create: SchedulingStrategy = new NodeLabelsSchedulingStrategy(Map.empty, new FifoSchedulingStrategyFactory)
      }

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingPipegraphs = Seq(
        pipegraphA.copy(labels = Set("unschedulable"))
      )

      startingPipegraphs.foreach(mockBl.insert)

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-b0", retryInterval, FiniteDuration(5, TimeUnit.SECONDS), schedulingStrategy = new TestNodeLabelSchedulingStrategyFactory)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-b0")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }


      fsm ! StartPipegraph(pipegraphA.name)

      expectMsgPF() {
        case PipegraphStarted(pipegraphA.name, _) =>
      }

      mockBl.instances().all().head.status should be(PipegraphStatus.UNSCHEDULABLE)

      fsm ! StopPipegraph(pipegraphA.name)

      expectMsgPF() {
        case PipegraphStopped(pipegraphA.name) =>
      }

      mockBl.instances().all().head.status should be(PipegraphStatus.STOPPED)


      system.stop(fsm)
      system.stop(collaborator)

    }

    "Recover unschedulables if strategy decision changes" in {

      class MySchedulingStrategyForTestsFactory extends SchedulingStrategyFactory {

        override def create: SchedulingStrategy = new SchedulingStrategy {
          override def choose(members: Set[Data.Collaborator], pipegraph: PipegraphModel): SchedulingStrategyOutcome = if (schedulable.get()) {
            Right((members.head, this))
          } else {
            Left(("Sorry unschedulable", this))
          }

        }

        val schedulable: AtomicBoolean = new AtomicBoolean(false)

      }



      val schedulingStrategy: MySchedulingStrategyForTestsFactory = new MySchedulingStrategyForTestsFactory


      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingPipegraphs = Seq(
        pipegraphA.copy(labels = Set("unschedulable"))
      )

      startingPipegraphs.foreach(mockBl.insert)

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-b1", retryInterval, 500.millis, schedulingStrategy = schedulingStrategy)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-b1")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }


      fsm ! StartPipegraph(pipegraphA.name)

      expectMsgPF() {
        case PipegraphStarted(pipegraphA.name, _) =>
      }

      mockBl.instances().all().head.status should be(PipegraphStatus.UNSCHEDULABLE)

      schedulingStrategy.schedulable.set(true)

      probe.expectMsg(WorkAvailable(pipegraphA.name))

      mockBl.instances().all().head.status should be(PipegraphStatus.PENDING)

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))

      probe.expectMsgPF() {
        case WorkGiven(_, _) =>
      }

      mockBl.instances().all().head.status should be(PipegraphStatus.PROCESSING)

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Do not forget unschedulables when restarting" in {


      @com.github.ghik.silencer.silent("never used")
      class MySchedulingStrategyForTestsFactory extends SchedulingStrategyFactory {

        override def create: SchedulingStrategy = new SchedulingStrategy {
          override def choose(members: Set[Data.Collaborator], pipegraph: PipegraphModel): SchedulingStrategyOutcome = if (schedulable.get()) {
            Right((members.head, this))
          } else {
            Left(("Sorry unschedulable", this))
          }

        }

        val schedulable: AtomicBoolean = new AtomicBoolean(false)

      }

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingPipegraphs = Seq(
        pipegraphA
      )

      startingPipegraphs.foreach(mockBl.insert)

      mockBl.instances().insert(instanceOf(pipegraphA, PipegraphStatus.UNSCHEDULABLE))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-b2", retryInterval, 500.millis)
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-b2")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))

      mockBl.instances().all().head.status should be(PipegraphStatus.PENDING)

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))

      probe.expectMsgPF() {
        case WorkGiven(_, _) =>
      }

      mockBl.instances().all().head.status should be(PipegraphStatus.PROCESSING)

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Inform worker of Stopping pipegraph" in {
      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)


      val startingPipegraphs = Seq(
        pipegraphA,
        pipegraphB,
        pipegraphC
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING),
        instanceOf(pipegraphB, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a4", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a4")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))
      probe.expectMsg(WorkAvailable(pipegraphB.name))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))

      probe.expectMsgType[WorkGiven]

      fsm ! StopPipegraph(pipegraphA.name)

      expectMsg(PipegraphStopped(pipegraphA.name))

      probe.expectMsg(ChildProtocol.CancelWork)

      val instances = mockBl.instances().instancesOf(pipegraphA.name)

      val uniqueAddress = SparkConsumersStreamingMasterGuardian.formatUniqueAddress(Cluster(system).selfUniqueAddress)
      val probeAddr = probe.ref.path.toString

      instances.head should matchPattern {
        case PipegraphInstanceModel(
        _,
        pipegraphA.name,
        1L,
        _,
        PipegraphStatus.STOPPING,
        Some(`uniqueAddress`),
        Some(`probeAddr`),
        None
        ) =>
      }

      probe.reply(ChildProtocol.WorkCancelled)

      eventually(timeout(Span(10, Seconds))) {
        mockBl.instances().instancesOf(pipegraphA.name) should matchPattern {
          case Seq(
          PipegraphInstanceModel(
          _,
          pipegraphA.name,
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

      val startingPipegraphs = Seq(
        pipegraphA
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a5", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a5")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))
      probe.reply(pipegraph.Protocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))
      probe.expectMsgType[WorkGiven]

      fsm ! StopPipegraph(pipegraphA.name)

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

      val startingPipegraphs = Seq(
        pipegraphA,
        pipegraphB
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING),
        instanceOf(pipegraphB, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a6", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a6")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))
      probe.expectMsg(WorkAvailable(pipegraphB.name))

      fsm ! StartPipegraph(pipegraphA.name)

      expectMsg(PipegraphNotStarted(pipegraphA.name, s"Cannot start more than one instance of [${pipegraphA.name}]"))

      system.stop(fsm)
      system.stop(collaborator)

    }

    "Give work to childrens that ask for it" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)


      val startingPipegraphs = Seq(
        pipegraphA,
        pipegraphB
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING),
        instanceOf(pipegraphB, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref
      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a7", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a7")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))
      probe.expectMsg(WorkAvailable(pipegraphB.name))

      fsm ! ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name)
      fsm ! ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphB.name)

      val work = expectMsgType[WorkGiven]

      work should matchPattern {
        case WorkGiven(
        `pipegraphA`,
        PipegraphInstanceModel(_, pipegraphA.name, 1L, _, PipegraphStatus.PROCESSING, _, _, None)
        ) =>
      }

      val work2 = expectMsgType[WorkGiven]
      work2 should matchPattern {
        case WorkGiven(
        `pipegraphB`,
        PipegraphInstanceModel(_, pipegraphB.name, 1L, _, PipegraphStatus.PROCESSING, _, _, None)
        ) =>
      }

      system.stop(fsm)
      system.stop(collaborator)

    }


    "Mark failed pipegraph and add exception to database" in {

      val mockBl = new MockPipegraphBl(new MockPipegraphInstanceBl)

      val startingPipegraphs = Seq(
        pipegraphA
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a9", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val collaborator = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a9")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))

      probe.expectMsgType[WorkGiven]

      val exception = new Exception("Work failed")

      probe.reply(WorkFailed(exception))

      val expected = ExceptionUtils.getStackTrace(exception)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(
          PipegraphInstanceModel(
          _,
          pipegraphA.name,
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

      val startingPipegraphs = Seq(
        pipegraphA
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref

      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-a10", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val _ = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-a10")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))

      probe.expectMsgType[WorkGiven]

      val exception = new Exception("Work failed")

      probe.reply(WorkFailed(exception))

      val expected = ExceptionUtils.getStackTrace(exception)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(
          PipegraphInstanceModel(
          _,
          pipegraphA.name,
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

      val startingPipegraphs = Seq(
        pipegraphA
      )

      val startingInstances = Seq(
        instanceOf(pipegraphA, PipegraphStatus.PENDING)
      )

      startingPipegraphs.foreach(mockBl.insert)
      startingInstances.foreach(mockBl.instances().insert(_))

      val probe = TestProbe()
      val childCreator: ChildCreator = (_, _, _) => probe.ref

      val watchdogCreator: ChildCreator = (master, name, _) => TestProbe(name)(system).ref
      val fsm = system.actorOf(
        SparkConsumersStreamingMasterGuardian.props(mockBl, watchdogCreator, "collaborator-11", retryInterval, FiniteDuration(5, TimeUnit.SECONDS))
      )

      val transitionProbe = TestProbe()

      transitionProbe.send(fsm, SubscribeTransitionCallBack(transitionProbe.ref))

      val _ = system.actorOf(CollaboratorActor.props(fsm, childCreator), "collaborator-11")

      transitionProbe.receiveWhile() {
        case CurrentState(_, Idle) =>
        case Transition(_, Idle, Initializing) =>
        case Transition(_, Initializing, Initialized) =>
      }

      probe.expectMsg(WorkAvailable(pipegraphA.name))

      probe.reply(ChildProtocol.GimmeWork(Cluster(system).selfUniqueAddress, pipegraphA.name))

      probe.expectMsgType[WorkGiven]

      probe.reply(WorkCompleted)

      eventually(timeout(Span(10, Seconds))) {

        mockBl.instances().all should matchPattern {
          case Seq(
          PipegraphInstanceModel(_, pipegraphA.name, 1L, _, PipegraphStatus.STOPPED, None, None, None)
          ) =>
        }
      }

    }

  }

}
