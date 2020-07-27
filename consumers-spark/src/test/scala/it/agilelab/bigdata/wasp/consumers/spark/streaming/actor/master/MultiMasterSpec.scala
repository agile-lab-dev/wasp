package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberUp}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.cluster.{Cluster, UniqueAddress}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.collaborator.CollaboratorActor
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian.ChildCreator
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.Protocol.GimmeWork
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.{MockPipegraphBl, MockPipegraphInstanceBl}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.repository.core.bl.PipegraphBL
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class MultiMasterSpec
    extends WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with Eventually
    with SystemUtils
    with TestData {

  import Protocol._
  import SparkConsumersStreamingMasterGuardian._

  import scala.concurrent.duration._

  def childCreatorFactory(probe: TestProbe): ChildCreator = {
    (_, name, factory) => {
      val candidateName = s"$name-${UUID.randomUUID()}"
      val saneName = URLEncoder.encode(candidateName.replaceAll(" ", "-"), StandardCharsets.UTF_8.name())
      factory.actorOf(Props(new HelperActor(probe.ref)), saneName)
    }
  }

  "Multi master mode" must {
    "schedule instances to nodes" in {

      import scala.concurrent.ExecutionContext.Implicits.global

      coordinator("coordinator") { (clusterC, _, testkitC, shutdownC) =>
        import testkitC._

        val mockBL: PipegraphBL = new MockPipegraphBl(new MockPipegraphInstanceBl)
        val probe               = TestProbe()


        val watchdogCreator: ChildCreator = (_, name, _) => TestProbe(name).ref

        mockBL.insert(pipegraph)
        mockBL.instances().insert(pipegraphInstance)

        val props = SparkConsumersStreamingMasterGuardian.props(mockBL, watchdogCreator, "collaborator", 1.millisecond)

        val childCreator = childCreatorFactory(probe)
        cluster("system-0", props, childCreator) { (cluster0, _, _, shutdown0) =>
          cluster("system-1", props, childCreator) { (cluster1, _, _, shutdown1) =>
            cluster("system-2", props, childCreator) { (cluster2, _, _, shutdown2) =>
              converge(clusterC, cluster0, cluster1, cluster2) {

                probe.expectMsgPF() {
                  case HelperEnvelope(address, sender, WorkAvailable("pipegraph-a")) =>
                    probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                }

                probe.expectMsgType[HelperEnvelope]

                mockBL.instances().all().foreach { i =>
                  i.status should be(PipegraphStatus.PROCESSING)

                  assert {
                    i.executedByNode === Some(
                      SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster0.selfUniqueAddress)
                    ) ||
                    i.executedByNode === Some(
                      SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster1.selfUniqueAddress)
                    ) ||
                    i.executedByNode === Some(
                      SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster2.selfUniqueAddress)
                    )
                  }

                }

                Future.traverse(Seq(shutdown0, shutdown1, shutdown2, shutdownC))(_.apply())
              }
            }
          }
        }
      }
    }

    "recover pipegraph that where running in non existent nodes" in {

      import scala.concurrent.ExecutionContext.Implicits.global

      coordinator("coordinator") { (clusterC, _, testkitC, shutdownC) =>
        import testkitC._

        val mockBL: PipegraphBL = new MockPipegraphBl(new MockPipegraphInstanceBl)
        val probe               = TestProbe()


        val childCreator = childCreatorFactory(probe)
        val watchdogCreator: ChildCreator = (_, name, _) => TestProbe(name).ref

        mockBL.insert(pipegraph)
        mockBL
          .instances()
          .insert(
            pipegraphInstance.copy(executedByNode = Some("NodeThatDoesNotExist"), status = PipegraphStatus.PROCESSING)
          )

        val props = SparkConsumersStreamingMasterGuardian.props(mockBL, watchdogCreator, "collaborator", 1.millisecond)

        cluster("system-0", props, childCreator) { (cluster0, _, _, shutdown0) =>
          cluster("system-1", props, childCreator) { (cluster1, _, _, shutdown1) =>
            cluster("system-2", props, childCreator) { (cluster2, _, _, shutdown2) =>
              converge(clusterC, cluster0, cluster1, cluster2) {

                probe.expectMsgPF() {
                  case HelperEnvelope(address, sender, WorkAvailable("pipegraph-a")) =>
                    probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                }

                probe.expectMsgType[HelperEnvelope]

                mockBL.instances().all().foreach { i =>
                  i.status should be(PipegraphStatus.PROCESSING)

                  assert {
                    i.executedByNode === Some(
                      SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster0.selfUniqueAddress)
                    ) ||
                    i.executedByNode === Some(
                      SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster1.selfUniqueAddress)
                    ) ||
                    i.executedByNode === Some(
                      SparkConsumersStreamingMasterGuardian.formatUniqueAddress(cluster2.selfUniqueAddress)
                    )
                  }
                }

                Future.traverse(Seq(shutdown0, shutdown1, shutdown2, shutdownC))(_.apply())
              }
            }
          }
        }
      }
    }

    "leave failed and stopped pipegraph alone" in {

      import scala.concurrent.ExecutionContext.Implicits.global

      coordinator("coordinator") { (clusterC, _, testkitC, shutdownC) =>
        import testkitC._

        val mockBL: PipegraphBL = new MockPipegraphBl(new MockPipegraphInstanceBl)
        val probe               = TestProbe()


        val childCreator = childCreatorFactory(probe)
        val watchdogCreator: ChildCreator = (_, name, _) => TestProbe(name).ref

        val pipegraphA = pipegraph.copy(name = "pipegraph-a")
        val pipegraphB = pipegraph.copy(name = "pipegraph-b")
        val pipegraphC = pipegraph.copy(name = "pipegraph-c")

        mockBL.insert(pipegraphA)
        mockBL.insert(pipegraphB)
        mockBL.insert(pipegraphC)

        val instanceA = pipegraphInstance.copy(
          name = "instance-a",
          instanceOf = pipegraphA.name,
          executedByNode = Some("NodeThatDoesNotExist"),
          status = PipegraphStatus.FAILED
        )

        val instanceB = instanceA.copy(
          instanceOf = pipegraphB.name,
          name = "instance-b",
          status = PipegraphStatus.STOPPED
        )

        val instanceC = instanceA.copy(
          name = "instance-c",
          instanceOf = pipegraphC.name,
          status = PipegraphStatus.STOPPING
        )

        mockBL.instances().insert(instanceA)
        mockBL.instances().insert(instanceB)
        mockBL.instances().insert(instanceC)

        val whoIsRunningTheSingletonProbe: TestProbe = TestProbe("who-is-running-the-singleton")

        val props = SparkConsumersStreamingMasterGuardian.props(
          mockBL,
          watchdogCreator,
          "collaborator",
          1.millisecond,
          Some(whoIsRunningTheSingletonProbe.ref)
        )

        cluster("system-0", props, childCreator) { (cluster0, _, _, shutdown0) =>
          cluster("system-1", props, childCreator) { (cluster1, _, _, shutdown1) =>
            cluster("system-2", props, childCreator) { (cluster2, _, _, shutdown2) =>
              converge(clusterC, cluster0, cluster1, cluster2) {

                whoIsRunningTheSingletonProbe.expectMsgType[UniqueAddress]
                whoIsRunningTheSingletonProbe.expectMsg(SparkConsumersStreamingMasterGuardian.InitializationCompleted)

                mockBL.instances().all().foreach { i =>
                  assert(i.status == PipegraphStatus.STOPPED || i.status == PipegraphStatus.FAILED)
                }

                Future.traverse(Seq(shutdown0, shutdown1, shutdown2, shutdownC))(_.apply())
              }
            }
          }
        }
      }
    }

    "Recover from failed nodes" in {

      import scala.concurrent.ExecutionContext.Implicits.global

      coordinator("coordinator") { (clusterC, _, testkitC, shutdownC) =>
        import testkitC._

        val mockBL: PipegraphBL = new MockPipegraphBl(new MockPipegraphInstanceBl)
        val probe               = TestProbe()

        val childCreator = childCreatorFactory(probe)
        val watchdogCreator: ChildCreator = (_, name, _) => TestProbe(name).ref

        mockBL.insert(pipegraph)
        mockBL.instances().insert(pipegraphInstance)

        val props = SparkConsumersStreamingMasterGuardian.props(mockBL, watchdogCreator, "collaborator", 1.millisecond)

        cluster("system-0", props, childCreator) { (cluster0, _, _, shutdown0) =>
          cluster("system-1", props, childCreator) { (cluster1, _, _, shutdown1) =>
            cluster("system-2", props, childCreator) { (cluster2, _, _, shutdown2) =>
              converge(clusterC, cluster0, cluster1, cluster2) {
                probe.expectMsgPF() {
                  case HelperEnvelope(address, sender, WorkAvailable("pipegraph-a")) =>
                    probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                }

                val address = probe.expectMsgPF() {
                  case HelperEnvelope(address, _, WorkGiven(_, _)) =>
                    address
                }

                Seq(cluster0, cluster1, cluster2).find(_.selfUniqueAddress == address).map { cluster =>
                  cluster.leave(cluster.selfAddress)
                }

                probe.expectMsgPF(Duration(120, "seconds")) {
                  case HelperEnvelope(address, sender, WorkAvailable("pipegraph-a")) =>
                    probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                }


                probe.expectMsgPF(Duration(120, "seconds")) {
                  case HelperEnvelope(address, sender, WorkGiven(_, _)) =>
                }

                mockBL.instances().all().foreach { i =>
                  i.status should be(PipegraphStatus.PROCESSING)
                  assert(i.executedByNode !== Some(SparkConsumersStreamingMasterGuardian.formatUniqueAddress(address)))

                }

                Future.traverse(Seq(shutdown0, shutdown1, shutdown2, shutdownC))(_.apply())
              }
            }
          }
        }
      }
    }

    "Recover from failed nodes not hosting MasterGuardian" in {

      import scala.concurrent.ExecutionContext.Implicits.global

      coordinator("coordinator") { (clusterC, _, testkitC, shutdownC) =>
        import testkitC._

        val mockBL: PipegraphBL = new MockPipegraphBl(new MockPipegraphInstanceBl)
        val probe               = TestProbe()

        val childCreator = childCreatorFactory(probe)
        val watchdogCreator: ChildCreator = (_, name, _) => TestProbe(name).ref

        val pipegraphA = pipegraph.copy(name = "pipegraph-a")
        val pipegraphB = pipegraph.copy(name = "pipegraph-b")
        val pipegraphC = pipegraph.copy(name = "pipegraph-c")

        mockBL.insert(pipegraphA)
        mockBL.insert(pipegraphB)
        mockBL.insert(pipegraphC)

        mockBL.instances().insert(pipegraphInstance.copy(name = "instance-a"))
        mockBL.instances().insert(pipegraphInstance.copy(name = "instance-b", instanceOf = pipegraphB.name))
        mockBL.instances().insert(pipegraphInstance.copy(name = "instance-c", instanceOf = pipegraphC.name))

        val whoIsRunningTheSingletonProbe: TestProbe = TestProbe("who-is-running-the-singleton")
        val props = SparkConsumersStreamingMasterGuardian.props(
          mockBL,
          watchdogCreator,
          "collaborator",
          1.millisecond,
          debugActor = Some(whoIsRunningTheSingletonProbe.ref)
        )

        cluster("system-0", props, childCreator) { (cluster0, _, _, shutdown0) =>
          cluster("system-1", props, childCreator) { (cluster1, _, _, shutdown1) =>
            cluster("system-2", props, childCreator) { (cluster2, _, _, shutdown2) =>
              converge(clusterC, cluster0, cluster1, cluster2) {

                val hostingSingleton = whoIsRunningTheSingletonProbe.expectMsgType[UniqueAddress]

                val nodeToPipegraph = multiple(6) {
                  probe.expectMsgPF() {
                    case HelperEnvelope(
                        address,
                        sender,
                        WorkAvailable("pipegraph-a") | WorkAvailable("pipegraph-b") | WorkAvailable("pipegraph-c")
                        ) =>
                      probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                      None
                    case HelperEnvelope(
                        _,
                        _,
                        GimmeWork(cluster0.selfUniqueAddress | cluster1.selfUniqueAddress | cluster2.selfUniqueAddress)
                        ) =>
                      None
                    case HelperEnvelope(
                        address,
                        _,
                        WorkGiven(pipegraph, _)
                        ) =>
                      Some((address, pipegraph.name))
                  }
                }.flatten.toMap

                val left = Seq(cluster0, cluster1, cluster2)
                  .find(_.selfUniqueAddress != hostingSingleton)
                  .map { c =>
                    c.leave(c.selfAddress)
                    c.selfUniqueAddress
                  }

                probe.expectMsgPF(Duration(120, "seconds")) {
                  case HelperEnvelope(address, sender, WorkAvailable("pipegraph-a" | "pipegraph-b" | "pipegraph-c")) =>
                    probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                }
                probe.expectMsgPF(Duration(120, "seconds")) {
                  case HelperEnvelope(_, _, WorkGiven(_,_)) =>
                }

                mockBL.instances().all().foreach { i =>
                  i.status should be(PipegraphStatus.PROCESSING)
                  assert(i.executedByNode !== left)
                }

                Future.traverse(Seq(shutdown0, shutdown1, shutdown2, shutdownC))(_.apply())
              }
            }
          }
        }
      }
    }

    "Reconnect to actors running pipegraph in the current node if MasterGuardian migrates" in {

      import scala.concurrent.ExecutionContext.Implicits.global

      coordinator("coordinator") { (clusterC, _, testkitC, shutdownC) =>
        import testkitC._

        val mockBL: PipegraphBL = new MockPipegraphBl(new MockPipegraphInstanceBl)
        val probe               = TestProbe()

        val childCreator = childCreatorFactory(probe)
        val watchdogCreator: ChildCreator = (_, name, _) => TestProbe(name).ref

        val pipegraphA = pipegraph.copy(name = "pipegraph-a")
        val pipegraphB = pipegraph.copy(name = "pipegraph-b")
        val pipegraphC = pipegraph.copy(name = "pipegraph-c")

        mockBL.insert(pipegraphA)
        mockBL.insert(pipegraphB)
        mockBL.insert(pipegraphC)

        val whoIsRunningTheSingletonProbe: TestProbe = TestProbe("who-is-running-the-singleton")

        val props = SparkConsumersStreamingMasterGuardian.props(
          mockBL,
          watchdogCreator,
          "collaborator",
          1.millisecond,
          Some(whoIsRunningTheSingletonProbe.ref)
        )

        cluster("system-0", props, childCreator) { (cluster0, proxy0, _, shutdown0) =>
          cluster("system-1", props, childCreator) { (cluster1, _, _, shutdown1) =>
            cluster("system-2", props, childCreator) { (cluster2, _, _, shutdown2) =>
              converge(clusterC, cluster0, cluster1, cluster2) {

                val firstOneRunningTheSingleton = whoIsRunningTheSingletonProbe.expectMsgType[UniqueAddress]

                proxy0 ! StartPipegraph(pipegraphA.name)
                proxy0 ! StartPipegraph(pipegraphB.name)
                proxy0 ! StartPipegraph(pipegraphC.name)

                val nodeToPipegraph = multiple(6) {
                  probe.expectMsgPF() {
                    case HelperEnvelope(
                        address,
                        sender,
                        WorkAvailable("pipegraph-a") | WorkAvailable("pipegraph-b") | WorkAvailable("pipegraph-c")
                        ) =>
                      probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                      None
                    case HelperEnvelope(
                        _,
                        _,
                        GimmeWork(cluster0.selfUniqueAddress | cluster1.selfUniqueAddress | cluster2.selfUniqueAddress)
                        ) =>
                      None
                    case HelperEnvelope(
                        address,
                        _,
                        WorkGiven(pipegraph, _)
                        ) =>
                      Some((address, pipegraph.name))
                  }
                }.flatten.toMap

                val left =
                  Seq(cluster0, cluster1, cluster2).find(_.selfUniqueAddress == firstOneRunningTheSingleton).map {
                    cluster =>
                      cluster.leave(cluster.selfAddress)
                      cluster.selfUniqueAddress
                  }

                probe.expectMsgPF(Duration(120, "seconds")) {
                  case HelperEnvelope(address, sender, WorkAvailable(pipegraphName))
                      if pipegraphName == nodeToPipegraph(firstOneRunningTheSingleton) =>
                    probe.sender() ! HelperEnvelope(address, sender, GimmeWork(address))
                }

                probe.expectMsgPF() {
                  case HelperEnvelope(
                      _,
                      _,
                      WorkGiven(_, _)
                      ) =>
                }

                mockBL.instances().all().foreach { i =>
                  i.status should be(PipegraphStatus.PROCESSING)
                  assert(i.executedByNode !== left)
                }

                Future.traverse(Seq(shutdown0, shutdown1, shutdown2, shutdownC))(_.apply())
              }
            }
          }
        }
      }
    }
  }
}

trait SystemUtils {

  type ShutdownCallback = () => Future[Terminated]

  def coordinator[A](
      configSubsection: String
  )(f: (Cluster, ActorRef, TestKit, ShutdownCallback) => Future[Seq[Terminated]]): Unit = {
    val system  = ActorSystem("WASP", ConfigFactory.load().getConfig(configSubsection))
    val cluster = Cluster(system)

    val proxySettings = ClusterSingletonProxySettings(system)
      .withSingletonName("singleton")
      .withRole("consumers-spark-streaming")

    val proxy = system.actorOf(ClusterSingletonProxy.props("singleton-manager", proxySettings))

    val shutdown: () => Future[Terminated] = () => {
      system.terminate()
    }

    Await.result(f(cluster, proxy, new TestKit(system), shutdown), Duration.Inf)
  }

  def cluster[A](configSubsection: String, singletonProps: Props, childCreator: ChildCreator)(
      f: (Cluster, ActorRef, TestKit, ShutdownCallback) => A
  ): A = {
    val system  = ActorSystem("WASP", ConfigFactory.load().getConfig(configSubsection))
    val cluster = Cluster(system)

    val managerSettings = ClusterSingletonManagerSettings(system)
      .withSingletonName("singleton")
      .withRole("consumers-spark-streaming")

    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = singletonProps,
        terminationMessage = PoisonPill,
        settings = managerSettings
      ),
      "singleton-manager"
    )

    val proxySettings = ClusterSingletonProxySettings(system)
      .withSingletonName("singleton")
      .withRole("consumers-spark-streaming")

    val proxy = system.actorOf(ClusterSingletonProxy.props("/user/singleton-manager", proxySettings))

    system.actorOf(CollaboratorActor.props(proxy, childCreator), "collaborator")

    val shutdown: () => Future[Terminated] = () => {
      system.terminate()
    }

    f(cluster, proxy, new TestKit(system), shutdown)
  }

  def converge[A](members: Cluster*)(f: => A)(implicit system: ActorSystem): A = {

    val probe = TestProbe()

    Cluster(system).subscribe(probe.ref, InitialStateAsEvents, classOf[MemberUp])

    probe.receiveN(members.length, Duration(120, "seconds"))

    f

  }

  def multiple[A](number: Int)(f: => A): List[A] = {
    Iterator.range(0, number).map(_ => f).toList
  }
}

trait TestData {

  val pipegraph: PipegraphModel = PipegraphModel(
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

  val pipegraphInstance: PipegraphInstanceModel =
    PipegraphInstanceModel("pipegraph-1", "pipegraph-a", 1L, 0L, PipegraphStatus.PENDING, None, None)
}

class HelperActor(upstream: ActorRef) extends Actor {
  val cluster: Cluster = Cluster(context.system)

  override def receive: Receive = {
    case msg @ HelperEnvelope(_, destination, message) =>
      destination ! message
    case msg =>
      upstream ! HelperEnvelope(cluster.selfUniqueAddress, sender(), msg)
  }
}

case class HelperEnvelope(uniqueAddress: UniqueAddress, sender: ActorRef, message: Any)
