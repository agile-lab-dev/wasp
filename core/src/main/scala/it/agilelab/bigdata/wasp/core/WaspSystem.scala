package it.agilelab.bigdata.wasp.core

import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.cluster.ClusterListenerActor
import it.agilelab.bigdata.wasp.repository.core.db.RepositoriesFactory
import it.agilelab.bigdata.wasp.core.kafka.NewKafkaAdminActor
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils._
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration

import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

object WaspSystem extends WaspConfiguration with Logging {
  // actor/singleton manager/proxy for master guardians
  val sparkConsumersBatchMasterGuardianName = "SparkConsumersBatchMasterGuardian"
  val sparkConsumersBatchMasterGuardianSingletonManagerName = "SparkConsumersBatchMasterGuardianSingletonManager"
  val sparkConsumersBatchMasterGuardianSingletonProxyName = "SparkConsumersBatchMasterGuardianSingletonProxy"
  val sparkConsumersBatchMasterGuardianRole = "consumers-spark-batch"
  val masterGuardianName = "MasterGuardian"
  val masterGuardianSingletonManagerName = "MasterGuardianSingletonManager"
  val masterGuardianSingletonProxyName = "MasterGuardianSingletonProxy"
  val masterGuardianRole = "master"
  val producersMasterGuardianName = "ProducersMasterGuardian"
  val producersMasterGuardianSingletonManagerName = "ProducersMasterGuardianSingletonManager"
  val producersMasterGuardianSingletonProxyName = "ProducersMasterGuardianSingletonProxy"
  val producersMasterGuardianRole = "producers"
  val sparkConsumersStreamingMasterGuardianName = "SparkConsumersStreamingMasterGuardian"
  val sparkConsumersStreamingMasterGuardianSingletonManagerName = "SparkConsumersStreamingMasterGuardianSingletonManager"
  val sparkConsumersStreamingMasterGuardianSingletonProxyName = "SparkConsumersStreamingMasterGuardianSingletonProxy"
  val sparkConsumersStreamingMasterGuardianRole = "consumers-spark-streaming"

  // actor/singleton manager/proxy names/roles for logger
  val loggerActorName = "LoggerActor"
  val loggerActorSingletonManagerName = "LoggerActorSingletonManager"
  val loggerActorSingletonProxyName = "LoggerActorSingletonProxy"
  val loggerActorRole = "logger"

  // producers topic for distributed publish subscribe
  val producersPubSubTopic = "producers"
  // BacklogSizeAnalizer Actor Path for messages redirection
  val telemetryPubSubTopic = "telemetryStreamingQueryProgress"

  // WASP actor system
  private var actorSystem_ : ActorSystem = _

  // proxies to cluster singletons of master guardians
  private var sparkConsumersBatchMasterGuardian_ : ActorRef = _
  private var masterGuardian_ : ActorRef = _
  private var producersMasterGuardian_ : ActorRef = _
  private var sparkConsumersStreamingMasterGuardian_ : ActorRef = _

  // proxy to singleton of logger actor
  private var loggerActor_ : ActorRef = _

  // actor refs of admin actors
  private var kafkaAdminActor_ : ActorRef = _

  // actor ref of clusterListener actor
  private var clusterListenerActor_ : ActorRef = _

  // distributed publish-subscribe mediator
  private var mediator_ : ActorRef = _

  // general timeout value, eg for actor's syncronous call (i.e. 'actor ? msg')
  val generalTimeout = Timeout(waspConfig.generalTimeoutMillis, TimeUnit.MILLISECONDS)

  // services timeout, used below
  val servicesTimeout = Timeout(waspConfig.servicesTimeoutMillis, TimeUnit.MILLISECONDS)

  /**
    * Initializes the WASP system if needed.
    *
    * @note Only the first call will initialize WASP; following attempts at initialization
    *       even if with different settings will not have any effect and will silently be ignored.
    */
  def initializeSystem(): Unit = WaspSystem.synchronized {
    if (actorSystem == null) {
      logger.info("Initializing WASP system")

      // initialize actor system
      logger.info("Initializing actor system")
      actorSystem_ = ActorSystem.create(waspConfig.actorSystemName, ConfigManager.conf)
      logger.info(s"Initialized actor system: $actorSystem")

      // create cluster singleton proxies to master guardians
      logger.info("Initializing proxies for master guardians")
      sparkConsumersBatchMasterGuardian_ = createSingletonProxy(sparkConsumersBatchMasterGuardianName, sparkConsumersBatchMasterGuardianSingletonProxyName, sparkConsumersBatchMasterGuardianSingletonManagerName, Seq(sparkConsumersBatchMasterGuardianRole))
      masterGuardian_ = createSingletonProxy(masterGuardianName, masterGuardianSingletonProxyName, masterGuardianSingletonManagerName, Seq(masterGuardianRole))
      producersMasterGuardian_ = createSingletonProxy(producersMasterGuardianName, producersMasterGuardianSingletonProxyName, producersMasterGuardianSingletonManagerName, Seq(producersMasterGuardianRole))
      sparkConsumersStreamingMasterGuardian_ = createSingletonProxy(sparkConsumersStreamingMasterGuardianName, sparkConsumersStreamingMasterGuardianSingletonProxyName, sparkConsumersStreamingMasterGuardianSingletonManagerName, Seq(sparkConsumersStreamingMasterGuardianRole))
      logger.info("Initialized proxies for master guardians")

      // create cluster singleton proxy to logger actor
      logger.info("Initializing proxy for logger actor")
      loggerActor_ = createSingletonProxy(loggerActorName, loggerActorSingletonProxyName, loggerActorSingletonManagerName, Seq(loggerActorRole))
      logger.info("Initialized proxy for logger actor")

      // spawn admin actors
      logger.info("Spawning admin actors")

      kafkaAdminActor_ =
          actorSystem.actorOf(Props(new NewKafkaAdminActor), NewKafkaAdminActor.name)
      logger.info("Spawned admin actors")

      // spawn clusterListener actor
      logger.info("Spawning clusterListener actor")
      clusterListenerActor_ = actorSystem.actorOf(Props(new ClusterListenerActor), ClusterListenerActor.name)
      logger.info("Spawned clusterListener actors")

      logger.info("Connecting to services")

      // check connectivity with kafka's zookeper
      val kafkaResult = kafkaAdminActor.ask(it.agilelab.bigdata.wasp.core.kafka.Initialization(ConfigManager.getKafkaConfig))((NewKafkaAdminActor.connectionTimeout + 1000).millis)
      val zkKafka = Await.ready(kafkaResult, servicesTimeout.duration)
      zkKafka.value match {
        case Some(Failure(t)) =>
          logger.error(t.getMessage)
          throw new Exception(t)

        case Some(Success(_)) =>
          logger.info("The system is connected with the Zookeeper cluster of Kafka")

        case None => throw new UnknownError("Unknown error during Zookeeper connection initialization")
      }

      logger.info("Connected to services")

      // get distributed pub sub mediator
      logger.info("Initializing distributed pub sub mediator")
      mediator_ = DistributedPubSub.get(WaspSystem.actorSystem).mediator
      logger.info("Initialized distributed pub sub mediator")

      logger.info("Initialized WASP system")
    } else {
      logger.warn("WASP already initialized, ignoring initialization request!")
    }
  }

  /**
    * Creates a cluster singleton proxy with the specified `singletonProxyName` for the WASP actor system.
    * The singleton is identified by the cluster singleton manager name & roles; the path to the cluster manager is
    * automatically built as "/user/`singletonManagerName`".
    */
  def createSingletonProxy(singletonName: String, singletonProxyName: String, singletonManagerName: String, roles: Seq[String]): ActorRef = {
    // helper for adding role to ClusterSingletonProxySettings
    val addRoleToSettings = (settings: ClusterSingletonProxySettings, role: String) => settings.withRole(role)

    // build the settings
    val settings = roles.foldLeft(ClusterSingletonProxySettings(actorSystem))(addRoleToSettings)

    // build the proxy
    val proxy = actorSystem.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/$singletonManagerName",
        settings = settings.withSingletonName(singletonName)),
      name = singletonProxyName)

    logger.info(s"Created cluster singleton proxy: $proxy")

    proxy
  }

  /**
    * Unique global shutdown point.
    */
  def shutdown(): Unit = {
    // close actor system
    if (actorSystem != null) actorSystem.terminate()

    // close wasp db connections
    RepositoriesFactory.service.getDB().close()
  }

  /**
    * Synchronous ask
    */
  def ??[T](actorReference: ActorRef, message: Any, duration: Option[FiniteDuration] = None): T = {

//    implicit val implicitSynchronousActorCallTimeout: Timeout = Timeout(duration.getOrElse(generalTimeout.duration))
//    Await.result(actorReference ? message, duration.getOrElse(generalTimeout.duration)).asInstanceOf[T]

    val durationInit = duration.getOrElse(generalTimeout.duration)

    // Manage the timeout in different ways:
    //  Start from initial duration (received or generalTimeout in configFile) and decrease it foreach encapsulated actor communication level
    //    * Xyz_C (AkkaHTTP Controller e.g. Pipegraph_C) to MasterGuardian => durationInit
    //    * MasterGuardian to XyzMasterGuardian => durationInit - 5s
    //    * XYZMasterGuardian to ... => durationInit - 10s
    //    ... maybe to extends ...
    import scala.concurrent.duration._
    val timeoutDuration: FiniteDuration = actorReference.path.name match {
      case WaspSystem.masterGuardianSingletonProxyName => durationInit
      case WaspSystem.sparkConsumersStreamingMasterGuardianSingletonProxyName |
           WaspSystem.sparkConsumersBatchMasterGuardianSingletonProxyName     |
           WaspSystem.producersMasterGuardianSingletonProxyName => durationInit - 5.seconds
      case _ => durationInit - 10.seconds
    }

    /* Only for Debug */
//    val timeoutDuration: FiniteDuration = actorReference.path.name match {
//      case WaspSystem.masterGuardianSingletonProxyName => durationInit * 2
//      case _ => durationInit
//    }

    implicit val implicitSynchronousActorCallTimeout: Timeout = Timeout(timeoutDuration)
    Await.result(actorReference ? message, timeoutDuration).asInstanceOf[T]
  }

  // accessors for actor system/refs, so we don't need public vars which may introduce bugs if someone reassigns stuff by accident
  implicit def actorSystem: ActorSystem = actorSystem_
  def sparkConsumersBatchMasterGuardian: ActorRef = sparkConsumersBatchMasterGuardian_
  def masterGuardian: ActorRef = masterGuardian_
  def producersMasterGuardian: ActorRef = producersMasterGuardian_
  def sparkConsumersStreamingMasterGuardian: ActorRef = sparkConsumersStreamingMasterGuardian_
  def loggerActor: ActorRef = loggerActor_
  def kafkaAdminActor: ActorRef = kafkaAdminActor_
  def clusterListenerActor: ActorRef = clusterListenerActor_
  def mediator: ActorRef = mediator_
}