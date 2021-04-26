package it.agilelab.bigdata.wasp.master.launcher

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, extractUri, handleExceptions, _}
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route, ValidationRejection}
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import it.agilelab.bigdata.nifi.client.{NifiClient, NifiRawClient}
import it.agilelab.bigdata.wasp.compiler.utils.{CompilerPool, FreeCodeCompiler}
import it.agilelab.bigdata.wasp.core.launcher.{ClusterSingletonLauncher, MasterCommandLineOptions}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, FreeCodeCompilerUtilsDefault, WaspConfiguration}
import it.agilelab.bigdata.wasp.core.{AroundLaunch, SystemPipegraphs, WaspSystem}
import it.agilelab.bigdata.wasp.master.MasterGuardian
import it.agilelab.bigdata.wasp.master.web.controllers._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import org.apache.avro.Schema
import org.apache.commons.cli
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang.exception.ExceptionUtils
import sttp.client.SttpBackend
import sttp.client.akkahttp.AkkaHttpBackend
import sttp.client.akkahttp.Types.LambdaFlow
import sttp.client.monad.FutureMonad

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import java.util.UUID
import java.util.concurrent.Executors
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * Launcher for the MasterGuardian and REST Server.
  * This trait is useful for who want extend the launcher
  *
  * @author Nicolò Bidotti
  */
trait MasterNodeLauncherTrait extends ClusterSingletonLauncher with WaspConfiguration with AroundLaunch {

  private val myExceptionHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        val resultJson = JsonResultsHelper.angularErrorBuilder(ExceptionUtils.getStackTrace(e)).toString()
        logger.error(s"Request to $uri could not be handled normally, result: $resultJson", e)
        complete(HttpResponse(InternalServerError, entity = resultJson))
      }
  }

  def beforeLaunch(): Unit = {
    addSystemPipegraphs()
    registerSchema()
  }

  def afterLaunch(): Unit = {
    startRestServer(WaspSystem.actorSystem, getRoutes)
    logger.info(s"MasterNode has been launched with WaspConfig ${waspConfig.toString}")
  }

  override def launch(commandLine: CommandLine): Unit = {
    beforeLaunch()
    super.launch(commandLine)
    afterLaunch()
  }

  /** Add system's schema to AvroSchemaManager.
    *
    * @return [[Seq[(Key, Schema)]]
    */
  def registerSchema(): Seq[(Long, Schema)] = Seq.empty

  private def addSystemPipegraphs(): Unit = {

    /* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
    ConfigBL.topicBL.insertIfNotExists(SystemPipegraphs.loggerTopic)
    ConfigBL.topicBL.insertIfNotExists(SystemPipegraphs.telemetryTopic)
    ConfigBL.indexBL.insertIfNotExists(SystemPipegraphs.solrLoggerIndex)
    ConfigBL.indexBL.insertIfNotExists(SystemPipegraphs.elasticLoggerIndex)
    ConfigBL.indexBL.insertIfNotExists(SystemPipegraphs.solrTelemetryIndex)
    ConfigBL.indexBL.insertIfNotExists(SystemPipegraphs.elasticTelemetryIndex)

    /* Event Engine */
    SystemPipegraphs.eventTopicModels.foreach(topicModel => ConfigBL.topicBL.upsert(topicModel))
    ConfigBL.pipegraphBL.insertIfNotExists(SystemPipegraphs.eventPipegraph)
    ConfigBL.indexBL.insertIfNotExists(SystemPipegraphs.eventIndex)
    ConfigBL.pipegraphBL.insertIfNotExists(SystemPipegraphs.mailerPipegraph)
    ConfigBL.topicBL.insertIfNotExists(SystemPipegraphs.eventMultiTopicModel)
    /* Producers */
    ConfigBL.producerBL.insertIfNotExists(SystemPipegraphs.loggerProducer)

    /* Pipegraphs */
    ConfigBL.pipegraphBL.insertIfNotExists(SystemPipegraphs.loggerPipegraph)

    ConfigBL.pipegraphBL.insertIfNotExists(SystemPipegraphs.telemetryPipegraph)
  }

  private def getRoutes: Route = {

    val clientExecutionContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
    val solrClient = SolrClient(ConfigManager.getSolrConfig)(clientExecutionContext)

    implicit val akkaBackend: SttpBackend[Future, Source[ByteString, Any], LambdaFlow] =
      AkkaHttpBackend.usingActorSystem(WaspSystem.actorSystem)(clientExecutionContext)
    implicit val monadForFuture: FutureMonad = new FutureMonad()(clientExecutionContext)
    implicit val serializer: SttpSerializer  = new SttpSerializer()

    val nifiConfig = ConfigManager.getNifiConfig

    val nifiApiUrl = nifiConfig.nifiBaseUrl + "/" + nifiConfig.nifiApiPath

    val proxyNifiUrl = waspConfig.restHttpsConf
      .map { _ =>
        s"https://${waspConfig.restServerHostname}:${waspConfig.restServerPort}/proxy/${nifiConfig.nifiUiPath}"
      }
      .getOrElse {
        s"http://${waspConfig.restServerHostname}:${waspConfig.restServerPort}/proxy/${nifiConfig.nifiUiPath}"
      }

    val nifiClient = new NifiClient(NifiRawClient(nifiApiUrl), UUID.randomUUID(), proxyNifiUrl)

    val nifiProxy = new NifiProxyController("proxy", nifiConfig.nifiBaseUrl)

    val compilerPool          = new CompilerPool(ConfigManager.getCompilerConfig.compilerInstances)
    val freeCodeCompiler      = new FreeCodeCompiler(compilerPool)
    val freeCodeCompilerUtils = new FreeCodeCompilerUtilsDefault(freeCodeCompiler)

    // Routes
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
      GenericController.getRoute ~
      StrategyController.getRoute ~
      new FreeCodeController(FreeCodeDBServiceDefault, freeCodeCompilerUtils).getRoute ~
      new LogsController(new DefaultSolrLogsService(solrClient)(clientExecutionContext)).getRoutes ~
      new EventController(new DefaultSolrEventsService(solrClient)(clientExecutionContext)).getRoutes ~
      new TelemetryController(new DefaultSolrTelemetryService(solrClient)(clientExecutionContext)).getRoutes ~
      new StatsController(new DefaultSolrStatsService(solrClient)(clientExecutionContext)).getRoutes ~
      new EditorController(
        new NifiEditorService(nifiClient)(clientExecutionContext),
        new DefaultPipegraphEditorService(freeCodeCompilerUtils)
      ).getRoutes ~
      nifiProxy.getRoutes ~
      additionalRoutes()
  }

  def additionalRoutes(): Route = reject

  private def startRestServer(actorSystem: ActorSystem, route: Route): Unit = {
    implicit val system: ActorSystem             = actorSystem
    implicit val materializer: ActorMaterializer = ActorMaterializer()

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
      val password: Array[Char] =
        scala.io.Source.fromFile(restHttpConfig.passwordLocation).getLines().next().toCharArray

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
