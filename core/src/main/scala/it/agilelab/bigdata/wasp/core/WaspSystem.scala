package it.agilelab.bigdata.wasp.core

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.core.elastic.ElasticAdminActor
import it.agilelab.bigdata.wasp.core.kafka.KafkaAdminActor

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.logging.{LoggerInjector, WaspLogger}
import it.agilelab.bigdata.wasp.core.solr.SolrAdminActor
import it.agilelab.bigdata.wasp.core.utils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


object WaspSystem {

  private val log = WaspLogger(this.getClass)
  var alreadyInit = false

  def systemInitialization(actorSystem: ActorSystem, force: Boolean = false) = {
    if (!alreadyInit || force) {
      alreadyInit = true

      //TODO Check if there the config of every component
      ConfigManager.initializeConfigs()

      val wasptimeout = conf.getDuration("default.timeout", TimeUnit.SECONDS)

      kafkaAdminActor = actorSystem.actorOf(Props(new KafkaAdminActor), KafkaAdminActor.name)

      elasticAdminActor = actorSystem.actorOf(Props(new ElasticAdminActor), ElasticAdminActor.name)

      solrAdminActor = actorSystem.actorOf(Props(new SolrAdminActor), SolrAdminActor.name)

      val kafkaResult = kafkaAdminActor.ask(it.agilelab.bigdata.wasp.core.kafka.Initialization(ConfigManager.getKafkaConfig))((KafkaAdminActor.connectionTimeout + 1000).millis)

      val zkKafka = Await.ready(kafkaResult, Duration(wasptimeout, TimeUnit.SECONDS))

      zkKafka.value match {
        case Some(Failure(t)) =>
          log.error(t.getMessage)
          throw new Exception(t)

        case Some(Success(_)) =>
          log.info("The system is connected with zookeeper of kafka")

        case None => throw new UnknownError("Unknown Error during zookeeper connection initialization")
      }

      val defaultDataStoreIndexed = Option(conf.getString("default.datastore.indexed"))

      /* Start the default indexed datastore. */

      defaultDataStoreIndexed.getOrElse("") match {
        case "elastic" => {
          log.info(s"Trying to connect with Elastic...")
          startupElastic(wasptimeout)
        }
        case "solr" => {
          log.info(s"Trying to connect with Solr...")
          startupSolr(wasptimeout)
        }
        case _ => {
          log.error("No Indexed datastore configurated!")
        }
      }

      val defaultDataStoreKeyValue = Option(conf.getString("default.datastore.keyvalue"))

      defaultDataStoreKeyValue.getOrElse("") match {
        case "hbase" => {
          log.info(s"Trying to connect with HBase...")
          startupHBase(wasptimeout)
        }
        case _ => {
          log.error("No KeyValue datastore configurated!")
        }
      }
      if (defaultDataStoreIndexed.isEmpty && defaultDataStoreKeyValue.isEmpty) {
        log.error("No datastore configurated!")
        throw new UnsupportedOperationException("No datastore configurated! Configure a KeyValue or a Indexed datastore")
      }
    }
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
   * Timeout value for actor's syncronous call (ex. 'actor ? msg') 
   */
  val conf = ConfigFactory.load
  val notimeout = conf.getBoolean("no_timeout")
  implicit val timeout = if (notimeout) Timeout(60, TimeUnit.MINUTES) else Timeout(60, TimeUnit.SECONDS)

  /**
   * WASP actor system.
   * Initialized by trait ActorSystemInjector through initializeActorSystem.
   */
  implicit var actorSystem: ActorSystem = _

  /**
    * Initializes the actor system if needed.
    *
    * @note Only the first call will initialize the actor system; following attempts at initialization
    *       even if with different settings will not have any effect and will silently be ignored.
    */
  def initializeActorSystem(actorSystemName: String): Unit = {
    /*
    We check for a null (not initialized) actor system two times:
    - one outside the synchronized block, so this method is cheap to call as it will be invoked
      when instantiating anything mixing in the ActorSystemInjector trait
    - one inside the synchronized block, as the outside one does not guarantee that it has not been
      initialized by someone else while we were blocked on the synchronized
     */
    if (actorSystem == null) WaspSystem.synchronized {
      if (actorSystem == null) {
        actorSystem = ActorSystem(actorSystemName)
      }
    }
  }

  /**
   * WASP logger actor.
   * Initialized by trait LoggerInjector through initializeLoggerActor.
   */
  var loggerActor: Option[ActorRef] = _

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

  //def loggerActorProps = Props[InternalLogProducerGuardian]
  var masterActor: ActorRef = _


  def now = System.currentTimeMillis

  /**
   * Unique global shutdown point.
   */
  def shutdown() = {

    // close actor system
    if (actorSystem != null)
      actorSystem.shutdown()

    // close wasp db connections
    WaspDB.getDB.close()
  }

  def getKafkaAdminActor = kafkaAdminActor

  var kafkaAdminActor: ActorRef = _
  var elasticAdminActor: ActorRef = _
  var solrAdminActor: ActorRef = _

  def ??[T](actorReference: ActorRef, message: WaspMessage, duration: Option[FiniteDuration] = None) =
    Await.result(actorReference ? message, duration.getOrElse(timeout.duration)).asInstanceOf[T]

}

trait WaspSystem extends ActorSystemInjector with LoggerInjector with ElasticConfiguration with SolrConfiguration {}
