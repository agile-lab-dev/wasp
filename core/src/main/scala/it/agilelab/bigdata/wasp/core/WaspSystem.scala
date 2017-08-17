package it.agilelab.bigdata.wasp.core

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.elastic.ElasticAdminActor
import it.agilelab.bigdata.wasp.core.kafka.KafkaAdminActor
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.solr.SolrAdminActor
import it.agilelab.bigdata.wasp.core.utils._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.{Failure, Success}


object WaspSystem extends WaspConfiguration {
  private val log = WaspLogger(this.getClass)
  
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
  
  /**
    * WASP actor system.
    * Initialized in initializeWaspSystem.
    */
  implicit var actorSystem: ActorSystem = _
  
  // proxies to cluster singletons of master guardians
  var batchMasterGuardian: ActorRef = _
  var masterGuardian: ActorRef = _
  var producersMasterGuardian: ActorRef = _
  var rtConsumersMasterGuardian: ActorRef = _
  var sparkConsumersMasterGuardian: ActorRef = _
  
  // proxy to singleton of logger actor
  var loggerActor: ActorRef = _
  
  // actor refs of admin actors
  var kafkaAdminActor: ActorRef = _
  var elasticAdminActor: ActorRef = _
  var solrAdminActor: ActorRef = _
  
  // distributed publish-subscribe mediator
  var mediator: ActorRef = _
  
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
      // initialize actor system
      actorSystem = ActorSystem.create(waspConfig.actorSystemName, ConfigManager.conf)
    
      // create cluster singleton proxies to master guardians
      batchMasterGuardian = createSingletonProxy(batchMasterGuardianSingletonProxyName, batchMasterGuardianSingletonManagerName, Seq(batchMasterGuardianRole))
      masterGuardian = createSingletonProxy(masterGuardianSingletonProxyName, masterGuardianSingletonManagerName, Seq(masterGuardianRole))
      producersMasterGuardian = createSingletonProxy(producersMasterGuardianSingletonProxyName, producersMasterGuardianSingletonManagerName, Seq(producersMasterGuardianRole))
      rtConsumersMasterGuardian = createSingletonProxy(rtConsumersMasterGuardianSingletonProxyName, rtConsumersMasterGuardianSingletonManagerName, Seq(rtConsumersMasterGuardianRole))
      sparkConsumersMasterGuardian = createSingletonProxy(sparkConsumersMasterGuardianSingletonProxyName, sparkConsumersMasterGuardianSingletonManagerName, Seq(sparkConsumersMasterGuardianRole))
  
      // create cluster singleton proxy to logger actor
      loggerActor = createSingletonProxy(loggerActorSingletonProxyName, loggerActorSingletonManagerName, Seq(loggerActorRole))
  
      // spawn admin actors
      kafkaAdminActor = actorSystem.actorOf(Props(new KafkaAdminActor), KafkaAdminActor.name)
      elasticAdminActor = actorSystem.actorOf(Props(new ElasticAdminActor), ElasticAdminActor.name)
      solrAdminActor = actorSystem.actorOf(Props(new SolrAdminActor), SolrAdminActor.name)
      
      // services timeout, used below
      val servicesTimeoutMillis = waspConfig.servicesTimeoutMillis
  
      // check connectivity with kafka's zookeper
      val kafkaResult = kafkaAdminActor.ask(it.agilelab.bigdata.wasp.core.kafka.Initialization(ConfigManager.getKafkaConfig))((KafkaAdminActor.connectionTimeout + 1000).millis)
      val zkKafka = Await.ready(kafkaResult, Duration(servicesTimeoutMillis, TimeUnit.SECONDS))
      zkKafka.value match {
        case Some(Failure(t)) =>
          log.error(t.getMessage)
          throw new Exception(t)
    
        case Some(Success(_)) =>
          log.info("The system is connected with zookeeper of kafka")
    
        case None => throw new UnknownError("Unknown Error during zookeeper connection initialization")
      }
    
      // implicit timeout used below
      implicit val implicitServicesTimeout = new Timeout(servicesTimeoutMillis, TimeUnit.MILLISECONDS)
    
      // initialize indexed datastore
      val defaultIndexedDatastore = waspConfig.defaultIndexedDatastore
      defaultIndexedDatastore match {
        case "elastic" => {
          log.info(s"Trying to connect with Elastic...")
          startupElastic(servicesTimeoutMillis)
        }
        case "solr" => {
          log.info(s"Trying to connect with Solr...")
          startupSolr(servicesTimeoutMillis)
        }
        case _ => {
          log.error("No Indexed datastore configurated!")
        }
      }
    
      // initialize keyvalue datastore
      val defaultKeyvalueDatastore = waspConfig.defaultKeyvalueDatastore
      defaultKeyvalueDatastore match {
        case "hbase" => {
          log.info(s"Trying to connect with HBase...")
          startupHBase(servicesTimeoutMillis)
        }
        case _ => {
          log.error("No KeyValue datastore configurated!")
        }
      }
      
      // TODO do we really want this? what if there is a rt-only pipegraph using just kafka?
      // fail if neither indexed nor keyvalue datastore is configured
      if (defaultIndexedDatastore.isEmpty && defaultKeyvalueDatastore.isEmpty) {
        log.error("No datastore configurated!")
        throw new UnsupportedOperationException("No datastore configurated! Configure a KeyValue or a Indexed datastore")
      }
  
      // get distributed pub sub mediator
      mediator = DistributedPubSub.get(WaspSystem.actorSystem).mediator
    }
  }
  
  /**
    * Creates a cluster singleton proxy with the specified `singletonProxyName` for the WASP actor system.
    * The singleton is identified by the cluster singleton manager name & roles; the path to the cluster manager is
    * automatically built as "/user/`singletonManagerName`".
    */
  def createSingletonProxy(singletonProxyName: String, singletonManagerName: String, roles: Seq[String]): ActorRef = {
    // helper for adding role to ClusterSingletonProxySettings
    val addRoleToSettings = (settings: ClusterSingletonProxySettings, role: String) => settings.withRole(role)
  
    // build the settings
    val settings = roles.foldLeft(ClusterSingletonProxySettings(actorSystem))(addRoleToSettings)
    
    // build the proxy
    actorSystem.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/$singletonManagerName",
        settings = settings),
      name = singletonProxyName)
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
        log.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        log.info("The system is connected with Elastic")

      case None => throw new UnknownError("Unknown Error during Elastic connection initialization")
    }
  }

  private def startupSolr(wasptimeout: Long)(implicit timeout: Timeout) = {
    //TODO if solrConfig are not initialized skip the initialization
    val solrResult = solrAdminActor ?  it.agilelab.bigdata.wasp.core.solr.Initialization(ConfigManager.getSolrConfig)
    val solrConnectionResult = Await.ready(solrResult, Duration(wasptimeout, TimeUnit.SECONDS))

    solrConnectionResult.value match {
      case Some(Failure(t)) =>
        log.error(t.getMessage)
        throw new Exception(t)

      case Some(Success(_)) =>
        log.info("The system is connected with Solr")

      case None => throw new UnknownError("Unknown Error during Solr connection initialization")
    }
  }
  
  /**
    * Initializes the logger actor if needed; safe to call multiple times.
    *
    * @note Only the first call will initialize the logger actor; following attempts at initialization
    *       even if with different settings will not have any effect and will silently be ignored.
    */
  def initializeLoggerActor(loggerActorProps: Props, loggerActorName: String): Unit = {
    /*
    We check for a null (not initialized) logger actor two times:
    - one outside the synchronized block, so this method is cheap to call as it will be invoked
      when instantiating anything mixing in the LoggerInjector trait
    - one inside the synchronized block, as the outside one does not guarantee that it has not been
      initialized by someone else while we were blocked on the synchronized
     */
    if (loggerActor == null) WaspSystem.synchronized {
      if (loggerActor == null) {
        if (actorSystem == null) {
          loggerActor = None
        } else {
          /*val actorPath = actorSystem / "InternalLogProducerGuardian"
					val future = actorSystem.actorSelection(actorPath).resolveOne()
					Some(Await.result(future, timeout.duration))*/
          loggerActor = Some(actorSystem.actorOf(loggerActorProps, loggerActorName))
        }
      }
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
  
  def ??[T](actorReference: ActorRef, message: WaspMessage, duration: Option[FiniteDuration] = None): T = {
    implicit val implicitSynchronousActorCallTimeout = synchronousActorCallTimeout
    Await.result(actorReference ? message, duration.getOrElse(synchronousActorCallTimeout.duration)).asInstanceOf[T]
  }
  
  
}