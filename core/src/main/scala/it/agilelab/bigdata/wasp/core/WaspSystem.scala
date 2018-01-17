package it.agilelab.bigdata.wasp.core

import java.util.ServiceLoader
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.kafka.KafkaAdminActor
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.plugins.WaspPlugin
import it.agilelab.bigdata.wasp.core.utils._

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.{Failure, Success}


object WaspSystem extends WaspConfiguration with Logging {
  // actor/singleton manager/proxy for master guardians
  val batchMasterGuardianName = "BatchMasterGuardian"
  val batchMasterGuardianSingletonManagerName = "BatchMasterGuardianSingletonManager"
  val batchMasterGuardianSingletonProxyName = "BatchMasterGuardianSingletonProxy"
  val batchMasterGuardianRole = "batch"
  val masterGuardianName = "MasterGuardian"
  val masterGuardianSingletonManagerName = "MasterGuardianSingletonManager"
  val masterGuardianSingletonProxyName = "MasterGuardianSingletonProxy"
  val masterGuardianRole = "master"
  val producersMasterGuardianName = "ProducersMasterGuardian"
  val producersMasterGuardianSingletonManagerName = "ProducersMasterGuardianSingletonManager"
  val producersMasterGuardianSingletonProxyName = "ProducersMasterGuardianSingletonProxy"
  val producersMasterGuardianRole = "producers"
  val rtConsumersMasterGuardianName = "RtConsumersMasterGuardian"
  val rtConsumersMasterGuardianSingletonManagerName = "RtConsumersMasterGuardianSingletonManager"
  val rtConsumersMasterGuardianSingletonProxyName = "RtConsumersMasterGuardianSingletonProxy"
  val rtConsumersMasterGuardianRole = "consumers-rt"
  val sparkConsumersMasterGuardianName = "SparkConsumersMasterGuardian"
  val sparkConsumersMasterGuardianSingletonManagerName = "SparkConsumersMasterGuardianSingletonManager"
  val sparkConsumersMasterGuardianSingletonProxyName = "SparkConsumersMasterGuardianSingletonProxy"
  val sparkConsumersMasterGuardianRole = "consumers-spark"
  
  // actor/singleton manager/proxy names/roles for logger
  val loggerActorName = "LoggerActor"
  val loggerActorSingletonManagerName = "LoggerActorSingletonManager"
  val loggerActorSingletonProxyName = "LoggerActorSingletonProxy"
  val loggerActorRole = "logger"
  
  // producers topic for distributed publish subscribe
  val producersPubSubTopic = "producers"
  
  // WASP actor system
  private var actorSystem_ : ActorSystem = _
  
  // proxies to cluster singletons of master guardians
  private var batchMasterGuardian_ : ActorRef = _
  private var masterGuardian_ : ActorRef = _
  private var producersMasterGuardian_ : ActorRef = _
  private var rtConsumersMasterGuardian_ : ActorRef = _
  private var sparkConsumersMasterGuardian_ : ActorRef = _
  
  // proxy to singleton of logger actor
  private var loggerActor_ : ActorRef = _
  
  // actor refs of admin actors
  private var kafkaAdminActor_ : ActorRef = _
  
  // distributed publish-subscribe mediator
  private var mediator_ : ActorRef = _
  
  // general timeout value, eg for actor's syncronous call (i.e. 'actor ? msg')
  val generalTimeout = Timeout(waspConfig.generalTimeoutMillis, TimeUnit.MILLISECONDS)
  
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
      batchMasterGuardian_ = createSingletonProxy(batchMasterGuardianName, batchMasterGuardianSingletonProxyName, batchMasterGuardianSingletonManagerName, Seq(batchMasterGuardianRole))
      masterGuardian_ = createSingletonProxy(masterGuardianName, masterGuardianSingletonProxyName, masterGuardianSingletonManagerName, Seq(masterGuardianRole))
      producersMasterGuardian_ = createSingletonProxy(producersMasterGuardianName, producersMasterGuardianSingletonProxyName, producersMasterGuardianSingletonManagerName, Seq(producersMasterGuardianRole))
      rtConsumersMasterGuardian_ = createSingletonProxy(rtConsumersMasterGuardianName, rtConsumersMasterGuardianSingletonProxyName, rtConsumersMasterGuardianSingletonManagerName, Seq(rtConsumersMasterGuardianRole))
      sparkConsumersMasterGuardian_ = createSingletonProxy(sparkConsumersMasterGuardianName, sparkConsumersMasterGuardianSingletonProxyName, sparkConsumersMasterGuardianSingletonManagerName, Seq(sparkConsumersMasterGuardianRole))
      logger.info("Initialized proxies for master guardians")
  
      // create cluster singleton proxy to logger actor
      logger.info("Initializing proxy for logger actor")
      loggerActor_ = createSingletonProxy(loggerActorName, loggerActorSingletonProxyName, loggerActorSingletonManagerName, Seq(loggerActorRole))
      logger.info("Initialized proxy for logger actor")
      
      // spawn admin actors
      logger.info("Spawning admin actors")
      kafkaAdminActor_ = actorSystem.actorOf(Props(new KafkaAdminActor), KafkaAdminActor.name)
      logger.info("Spawned admin actors")
      
      logger.info("Finding WASP plugins")
      val pluginLoader: ServiceLoader[WaspPlugin] = ServiceLoader.load[WaspPlugin](classOf[WaspPlugin])
      val plugins = pluginLoader.iterator().asScala.toList
      logger.info(s"Found ${plugins.size} plugins")
      logger.info("Initializing plugins")
      plugins foreach {
        plugin => {
          logger.info(s"Initializing plugin ${plugin.getClass.getSimpleName}")
          plugin.initialize()
        }
      }
  
      logger.info("Connecting to services")
  
      // services timeout, used below
      val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis

      // check connectivity with kafka's zookeper
      val kafkaResult = kafkaAdminActor.ask(it.agilelab.bigdata.wasp.core.kafka.Initialization(ConfigManager.getKafkaConfig))((KafkaAdminActor.connectionTimeout + 1000).millis)
      val zkKafka = Await.ready(kafkaResult, Duration(servicesTimeoutMillis, TimeUnit.SECONDS))
      zkKafka.value match {
        case Some(Failure(t)) =>
          logger.error(t.getMessage)
          throw new Exception(t)
    
        case Some(Success(_)) =>
          logger.info("The system is connected with the Zookeeper cluster of Kafka")
    
        case None => throw new UnknownError("Unknown error during Zookeeper connection initialization")
      }

      // implicit timeout used below
      implicit val implicitServicesTimeout = new Timeout(servicesTimeoutMillis, TimeUnit.MILLISECONDS)
    
      // initialize indexed datastore
      val defaultIndexedDatastore = waspConfig.defaultIndexedDatastore
      if (defaultIndexedDatastore != "elastic" && defaultIndexedDatastore != "solr") {
        logger.error(s"No indexed datastore configured! Value: ${defaultIndexedDatastore} is different from elastic or solr")
      }

      // initialize keyvalue datastore
      val defaultKeyvalueDatastore = waspConfig.defaultKeyvalueDatastore
      defaultKeyvalueDatastore match {
        case "hbase" => {
          logger.info(s"Trying to connect with HBase...")
          startupHBase(servicesTimeoutMillis)
        }
        case _ => {
          logger.error("No keyvalue datastore configured!")
        }
      }
      
      // TODO do we really want this? what if there is a rt-only pipegraph using just kafka?
      // fail if neither indexed nor keyvalue datastore is configured
      if (defaultIndexedDatastore.isEmpty && defaultKeyvalueDatastore.isEmpty) {
        logger.error("No datastore configured!")
        throw new UnsupportedOperationException("No datastore configured! Configure a keyvalue or an indexed datastore")
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

  private def startupHBase(wasptimeout: Long) = {
    //TODO Initialize the HBase configurations and test if It's up
  }

  
  /**
    * Unique global shutdown point.
    */
  def shutdown(): Unit = {
    // close actor system
    if (actorSystem != null) actorSystem.terminate()

    // close wasp db connections
    WaspDB.getDB.close()
  }
  
  /**
    * Synchronous ask
    */
  def ??[T](actorReference: ActorRef, message: Any, duration: Option[FiniteDuration] = None): T = {

//    implicit val implicitSynchronousActorCallTimeout: Timeout = Timeout(duration.getOrElse(generalTimeout.duration))
//    Await.result(actorReference ? message, duration.getOrElse(generalTimeout.duration)).asInstanceOf[T]

    val durationInit = duration.getOrElse(generalTimeout.duration)

    // TODO complete
    // Manage the timeout in several ways:
    //  Start from initial duration (received or generalTimeout in configFile) and decrease it foreach encapsulated actor communication level
    //    Xyz_C (AkkaHTTP Controller e.g. Pipegraph_C) => durationInit
    //    MasterGuardian to XyzMasterGuardian => durationInit - 5s
    //    XYZMasterGuardian to ... => durationInit - 10s
    //    ...
    import scala.concurrent.duration._
    val newDuration:FiniteDuration = actorReference.path.name match {
      case WaspSystem.masterGuardianSingletonProxyName => durationInit
      case _ => durationInit - 5.seconds
    }

    // Only for Debug
//    val newDuration:FiniteDuration = actorReference.path.name match {
//      case WaspSystem.masterGuardianSingletonProxyName => durationInit * 2
//      case _ => durationInit
//    }

    implicit val implicitSynchronousActorCallTimeout: Timeout = Timeout(newDuration)
    Await.result(actorReference ? message, newDuration).asInstanceOf[T]
  }
  
  // accessors for actor system/refs, so we don't need public vars which may introduce bugs if someone reassigns stuff by accident
  implicit def actorSystem: ActorSystem = actorSystem_
  def batchMasterGuardian: ActorRef = batchMasterGuardian_
  def masterGuardian: ActorRef = masterGuardian_
  def producersMasterGuardian: ActorRef = producersMasterGuardian_
  def rtConsumersMasterGuardian: ActorRef = rtConsumersMasterGuardian_
  def sparkConsumersMasterGuardian: ActorRef = sparkConsumersMasterGuardian_
  def loggerActor: ActorRef = loggerActor_
  def kafkaAdminActor: ActorRef = kafkaAdminActor_
  def mediator: ActorRef = mediator_
}