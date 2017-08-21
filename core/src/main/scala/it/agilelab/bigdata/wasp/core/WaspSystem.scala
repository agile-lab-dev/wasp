package it.agilelab.bigdata.wasp.core

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.elastic.ElasticAdminActor
import it.agilelab.bigdata.wasp.core.kafka.KafkaAdminActor
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.solr.SolrAdminActor
import it.agilelab.bigdata.wasp.core.utils._

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
  private var elasticAdminActor_ : ActorRef = _
  private var solrAdminActor_ : ActorRef = _
  
  // distributed publish-subscribe mediator
  private var mediator_ : ActorRef = _
  
  // timeout value for actor's syncronous call (ex. 'actor ? msg')
  val synchronousActorCallTimeout = Timeout(waspConfig.generalTimeoutMillis, TimeUnit.MILLISECONDS)
  
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
      elasticAdminActor_ = actorSystem.actorOf(Props(new ElasticAdminActor), ElasticAdminActor.name)
      solrAdminActor_ = actorSystem.actorOf(Props(new SolrAdminActor), SolrAdminActor.name)
      logger.info("Spawned admin actors")
  
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
      defaultIndexedDatastore match {
        case "elastic" => {
          logger.info(s"Trying to connect with Elastic...")
          startupElastic(servicesTimeoutMillis)
        }
        case "solr" => {
          logger.info(s"Trying to connect with Solr...")
          startupSolr(servicesTimeoutMillis)
        }
        case _ => {
          logger.error("No indexed datastore configured!")
        }
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


  private def startupElastic(wasptimeout: Long)(implicit timeout: Timeout) = {
    //TODO if elasticConfig are not initialized skip the initialization
    val elasticResult = elasticAdminActor ?  it.agilelab.bigdata.wasp.core.elastic.Initialization(ConfigManager.getElasticConfig)

    //TODO remove infinite waiting and enable index swapping
    val elasticConnectionResult = Await.ready(elasticResult, Duration(wasptimeout, TimeUnit.SECONDS))

    elasticConnectionResult.value match {
      case Some(Failure(t)) =>
        logger.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        logger.info("The system is connected with Elastic")

      case None => throw new UnknownError("Unknown error during Elastic connection initialization")
    }
  }

  private def startupSolr(wasptimeout: Long)(implicit timeout: Timeout) = {
    //TODO if solrConfig are not initialized skip the initialization
    val solrResult = solrAdminActor ?  it.agilelab.bigdata.wasp.core.solr.Initialization(ConfigManager.getSolrConfig)
    val solrConnectionResult = Await.ready(solrResult, Duration(wasptimeout, TimeUnit.SECONDS))

    solrConnectionResult.value match {
      case Some(Failure(t)) =>
        logger.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        logger.info("The system is connected with Solr")

      case None => throw new UnknownError("Unknown error during Solr connection initialization")
    }
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
    implicit val implicitSynchronousActorCallTimeout = synchronousActorCallTimeout
    Await.result(actorReference ? message, duration.getOrElse(synchronousActorCallTimeout.duration)).asInstanceOf[T]
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
  def elasticAdminActor: ActorRef = elasticAdminActor_
  def solrAdminActor: ActorRef = solrAdminActor_
  def mediator: ActorRef = mediator_
}