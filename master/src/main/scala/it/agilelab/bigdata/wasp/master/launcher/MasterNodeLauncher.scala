package it.agilelab.bigdata.wasp.master.launcher

import java.io.{FileInputStream, InputStream}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives.{complete, extractUri, handleExceptions, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.{ClusterSingletonLauncher, MasterCommandLineOptions}
import it.agilelab.bigdata.wasp.core.models.{IndexModel, PipegraphModel, ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration
import it.agilelab.bigdata.wasp.core.{SystemPipegraphs, WaspSystem}
import it.agilelab.bigdata.wasp.master.MasterGuardian
import it.agilelab.bigdata.wasp.master.web.controllers.Status_C.helpApi
import it.agilelab.bigdata.wasp.master.web.controllers._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.httpResponseJson
import org.apache.commons.cli
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang.exception.ExceptionUtils

import scala.io.Source

/**
	* Launcher for the MasterGuardian and REST Server.
	* This trait is useful for who want extend the launcher
	* @author Nicolò Bidotti
	*/
trait MasterNodeLauncherTrait extends ClusterSingletonLauncher with WaspConfiguration {

	override def launch(commandLine: CommandLine): Unit = {
		addSystemPipegraphs()
		super.launch(commandLine)
		startRestServer(WaspSystem.actorSystem, getRoutes)
		logger.info(s"MasterNode has been launched with WaspConfig ${waspConfig.toString}")
	}
	
	override def getSingletonProps: Props = Props(new MasterGuardian(ConfigBL))
	
	override def getSingletonName: String = WaspSystem.masterGuardianName
	
	override def getSingletonManagerName: String = WaspSystem.masterGuardianSingletonManagerName
	
	override def getSingletonRoles: Seq[String] = Seq(WaspSystem.masterGuardianRole)
	
	private def addSystemPipegraphs(): Unit = {

		/* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
		waspDB.insertIfNotExists[TopicModel](SystemPipegraphs.loggerTopic)
		waspDB.insertIfNotExists[TopicModel](SystemPipegraphs.telemetryTopic)
		waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.solrLoggerIndex)
		waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.elasticLoggerIndex)
		waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.solrTelemetryIndex)
		waspDB.insertIfNotExists[IndexModel](SystemPipegraphs.elasticTelemetryIndex)

		/* Producers */
		waspDB.insertIfNotExists[ProducerModel](SystemPipegraphs.loggerProducer)

		/* Pipegraphs */
		waspDB.insertIfNotExists[PipegraphModel](SystemPipegraphs.loggerPipegraph)

		waspDB.insertIfNotExists[PipegraphModel](SystemPipegraphs.telemetryPipegraph)
	}
	
	private val myExceptionHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        val resultJson = JsonResultsHelper.angularErrorBuilder(ExceptionUtils.getStackTrace(e)).toString()
        logger.error(s"Request to $uri could not be handled normally, result: $resultJson", e)
        complete(HttpResponse(InternalServerError, entity = resultJson))
      }
  }

	private def getRoutes: Route = {
		BatchJob_C.getRoute ~
		Configuration_C.getRoute ~
		Index_C.getRoute ~
		MlModels_C.getRoute ~
		Pipegraph_C.getRoute ~
		Producer_C.getRoute ~
		Topic_C.getRoute ~
		Status_C.getRoute ~
			additionalRoutes()
	}

	def additionalRoutes(): Route = pass(complete(httpResponseJson(entity = helpApi.prettyPrint)))
	
	private def startRestServer(actorSystem: ActorSystem, route: Route): Unit = {
		implicit val system = actorSystem
		implicit val materializer = ActorMaterializer()
		val finalRoute = handleExceptions(myExceptionHandler)(route)
		logger.info(s"start rest server and bind on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}")

		val optHttpsContext = createHttpsContext

		val bindingFuture = if(optHttpsContext.isDefined) {
			logger.info(s"Rest API will be available through HTTPS on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}")
			Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort, optHttpsContext.get)
		} else {
			logger.info(s"Rest API will be available through HTTP on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}")
			Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort)
		}
	}

	private def createHttpsContext: Option[HttpsConnectionContext] = {
		if(waspConfig.restHttpsConf.isEmpty){
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
	
	override def getNodeName: String = "master"
	
	override def getOptions: Seq[cli.Option] = super.getOptions ++ MasterCommandLineOptions.allOptions
}

/**
	*
	* Create the main static method to run
	*/
object MasterNodeLauncher extends MasterNodeLauncherTrait