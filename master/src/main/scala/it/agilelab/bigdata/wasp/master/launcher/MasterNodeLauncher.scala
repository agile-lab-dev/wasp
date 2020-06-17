package it.agilelab.bigdata.wasp.master.launcher

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import java.util.concurrent.{Executors, ThreadFactory}

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives.{complete, extractUri, handleExceptions, _}
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route, ValidationRejection}
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.stream.ActorMaterializer
import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.eventengine.Event
import it.agilelab.bigdata.wasp.core.eventengine.eventproducers.EventPipegraphModel
import it.agilelab.bigdata.wasp.core.launcher.{ClusterSingletonLauncher, MasterCommandLineOptions}
import it.agilelab.bigdata.wasp.core.models.{IndexModel, MultiTopicModel, PipegraphModel, ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, FreeCodeCompilerUtilsDefault, WaspConfiguration}
import it.agilelab.bigdata.wasp.core.{SystemPipegraphs, WaspSystem}
import it.agilelab.bigdata.wasp.master.MasterGuardian
import it.agilelab.bigdata.wasp.master.web.controllers.Status_C.helpApi
import it.agilelab.bigdata.wasp.master.web.controllers._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.httpResponseJson
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import org.apache.avro.Schema
import org.apache.commons.cli
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang.exception.ExceptionUtils

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.io.Source

/**
  * Launcher for the MasterGuardian and REST Server.
  * This trait is useful for who want extend the launcher
  *
  * @author Nicolò Bidotti
  */
trait MasterNodeLauncherTrait extends ClusterSingletonLauncher with WaspConfiguration {

  private val myExceptionHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        val resultJson = JsonResultsHelper.angularErrorBuilder(ExceptionUtils.getStackTrace(e)).toString()
        logger.error(s"Request to $uri could not be handled normally, result: $resultJson", e)
        complete(HttpResponse(InternalServerError, entity = resultJson))
      }
  }

  override def launch(commandLine: CommandLine): Unit = {
    addSystemPipegraphs()
    registerSchema()
    super.launch(commandLine)
    startRestServer(WaspSystem.actorSystem, getRoutes)
    logger.info(s"MasterNode has been launched with WaspConfig ${waspConfig.toString}")
  }

  /** Add system's schema to AvroSchemaManager.
    *
    * @return [[Seq[(Key, Schema)]]
    */
  def registerSchema(): Seq[(Long, Schema)] = {
    val schemas = Seq(AvroSchema[Event])
    if (schemas.isEmpty) {
      Seq.empty
    } else {
      val configAvroSchemaManager = ConfigManager.getAvroSchemaManagerConfig
      AvroSchemaManagerFactory.initialize(configAvroSchemaManager).registerAll(schemas)
    }
  }

  private def addSystemPipegraphs(): Unit = {

    /* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
    waspDB.insertIfNotExists[TopicModel](SystemPipegraphs.loggerTopic)
    waspDB.insertIfNotExists[TopicModel](SystemPipegraphs.telemetryTopic)
    waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.solrLoggerIndex)
    waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.elasticLoggerIndex)
    waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.solrTelemetryIndex)
    waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.elasticTelemetryIndex)

    /* Event Engine */
    SystemPipegraphs.eventTopicModels.foreach(topicModel => waspDB.upsert[TopicModel](topicModel))
    waspDB.insertIfNotExists[PipegraphModel](SystemPipegraphs.eventPipegraph)
    waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.eventIndex)
    waspDB.insertIfNotExists[PipegraphModel](SystemPipegraphs.mailerPipegraph)
    waspDB.insertIfNotExists[MultiTopicModel](SystemPipegraphs.eventMultiTopicModel)
    /* Producers */
    waspDB.insertIfNotExists[ProducerModel](SystemPipegraphs.loggerProducer)

    /* Pipegraphs */
    waspDB.insertIfNotExists[PipegraphModel](SystemPipegraphs.loggerPipegraph)

    waspDB.insertIfNotExists[PipegraphModel](SystemPipegraphs.telemetryPipegraph)
  }

  private def getRoutes: Route = {

    val solrExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
    val solrClient                                     = SolrClient(ConfigManager.getSolrConfig)(solrExecutionContext)
    new BatchJobController(DefaultBatchJobService).getRoute ~
      Configuration_C.getRoute ~
      Index_C.getRoute ~
      MlModels_C.getRoute ~
      Pipegraph_C.getRoute ~
      Producer_C.getRoute ~
      Topic_C.getRoute ~
      Status_C.getRoute ~
      Document_C.getRoute ~
      KeyValueController.getRoute ~
      RawController.getRoute ~
      new FreeCodeController(FreeCodeDBServiceDefault,FreeCodeCompilerUtilsDefault).getRoute ~
      new LogsController(new DefaultSolrLogsService(solrClient)(solrExecutionContext)).getRoutes ~
      new EventController(new DefaultSolrEventsService(solrClient)(solrExecutionContext)).getRoutes ~
      new TelemetryController(new DefaultSolrTelemetryService(solrClient)(solrExecutionContext)).getRoutes ~
      new StatsController(new DefaultSolrStatsService(solrClient)(solrExecutionContext)).getRoutes ~
      additionalRoutes()
  }

  def additionalRoutes(): Route = reject

  private def startRestServer(actorSystem: ActorSystem, route: Route): Unit = {
    implicit val system       = actorSystem
    implicit val materializer = ActorMaterializer()

    val rejectionHandler = RejectionHandler
      .newBuilder()
      .handle {
        case rejection @ ValidationRejection(message, _) =>
          extractRequest { request =>
            complete {
              logger.error(s"request ${request} was rejected with rejection ${rejection}")
              HttpResponse(StatusCodes.BadRequest, entity = HttpEntity(s"Input validation failed: ${message}"))
            }
          }
        case rejection =>
          extractRequest { request =>
            complete {
              logger.error(s"request ${request} was rejected with rejection ${rejection}")
              HttpResponse(StatusCodes.BadRequest, entity = HttpEntity("Request was not recognized"))
            }
          }
      }
      .result()

    val finalRoute = handleExceptions(myExceptionHandler) {
      handleRejections(rejectionHandler) {
        route
      }
    }

    logger.info(s"start rest server and bind on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}")

    val optHttpsContext = createHttpsContext

    val bindingFuture = if (optHttpsContext.isDefined) {
      logger.info(
        s"Rest API will be available through HTTPS on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}"
      )
      Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort, optHttpsContext.get)
    } else {
      logger.info(
        s"Rest API will be available through HTTP on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}"
      )
      Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort)
    }
  }

  private def createHttpsContext: Option[HttpsConnectionContext] = {
    if (waspConfig.restHttpsConf.isEmpty) {
      None
    } else {

      logger.info("Creating Https context for REST API")

      val restHttpConfig = waspConfig.restHttpsConf.get

      logger.info(s"Reading keystore password from file ${restHttpConfig.passwordLocation}")
      val password: Array[Char] = Source.fromFile(restHttpConfig.passwordLocation).getLines().next().toCharArray

      val ks: KeyStore = KeyStore.getInstance(restHttpConfig.keystoreType)

      val keystore: FileInputStream = new FileInputStream(restHttpConfig.keystoreLocation)

      require(keystore != null, "Keystore required!")
      ks.load(keystore, password)

      val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
      keyManagerFactory.init(ks, password)

      val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
      tmf.init(ks)

      val sslContext: SSLContext = SSLContext.getInstance("TLS")
      sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
      val https: HttpsConnectionContext = ConnectionContext.https(sslContext)
      Some(https)
    }
  }

  override def getSingletonProps: Props = Props(new MasterGuardian(ConfigBL))

  override def getSingletonName: String = WaspSystem.masterGuardianName

  override def getSingletonManagerName: String = WaspSystem.masterGuardianSingletonManagerName

  override def getSingletonRoles: Seq[String] = Seq(WaspSystem.masterGuardianRole)

  override def getNodeName: String = "master"

  override def getOptions: Seq[cli.Option] = super.getOptions ++ MasterCommandLineOptions.allOptions
}

/**
  *
  * Create the main static method to run
  */
object MasterNodeLauncher extends MasterNodeLauncherTrait
