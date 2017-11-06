package it.agilelab.bigdata.wasp.master.launcher

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives.{complete, extractUri, handleExceptions, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher
import it.agilelab.bigdata.wasp.core.models.{IndexModel, PipegraphModel, ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, MongoDBHelper, WaspConfiguration, WaspDB}
import it.agilelab.bigdata.wasp.core.{SystemPipegraphs, WaspSystem}
import it.agilelab.bigdata.wasp.master.MasterGuardian
import it.agilelab.bigdata.wasp.master.web.controllers.Status_C.helpApi
import it.agilelab.bigdata.wasp.master.web.controllers._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.httpResponseJson
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
	* Launcher for the MasterGuardian and REST API.
	* This trait is useful for who want extend the launcher
	* @author Nicolò Bidotti
	*/
trait MasterNodeLauncherTrait extends ClusterSingletonLauncher with WaspConfiguration {
		// parse command line
		val options = parseCommandLine(args.toList)
	override def launch(commandLine: CommandLine): Unit = {
		
		// drop db if needed
		if (options.dropDb) {
			val config = ConfigManager.getMongoDBConfig
			logger.info(s"Dropping MongoDB database ${config.databaseName}")
			val mongoDb = MongoDBHelper.getDatabase(config)
			val dropFuture = mongoDb.drop().toFuture()
			Await.result(dropFuture, Duration(10, TimeUnit.SECONDS))
		}
		
		// add system pipegraphs
		addSystemPipegraphs()
		
		// launch cluster singleton
		super.launch(commandLine)

		// launch rest server
		startRestServer(WaspSystem.actorSystem, getRoutes)
	}
	
	override def getSingletonProps: Props = Props(new MasterGuardian(ConfigBL))
	
	override def getSingletonName: String = WaspSystem.masterGuardianName
	
	override def getSingletonManagerName: String = WaspSystem.masterGuardianSingletonManagerName
	
	override def getSingletonRoles: Seq[String] = Seq(WaspSystem.masterGuardianRole)
	
	private def addSystemPipegraphs(): Unit = {
		val db = WaspDB.getDB
		
		// add logger pipegraph
		db.insertIfNotExists[TopicModel](SystemPipegraphs.loggerTopic)
		db.insertIfNotExists[ProducerModel](SystemPipegraphs.loggerProducer)
		db.insertIfNotExists[IndexModel](SystemPipegraphs.loggerIndex)
		db.insertIfNotExists[PipegraphModel](SystemPipegraphs.loggerPipegraph)
		
		// add raw pipegraph
		db.insertIfNotExists[TopicModel](SystemPipegraphs.rawTopic)
		db.insertIfNotExists[IndexModel](SystemPipegraphs.rawIndex)
		db.insertIfNotExists[PipegraphModel](SystemPipegraphs.rawPipegraph)
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
		MlModels_C.getRoute  ~
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
		val bindingFuture = Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort)
	}
	
	override def getNodeName: String = "master"
	
	private case class Options(dropDb: Boolean = false)
	
	private def parseCommandLine(args: List[String], options: Options = Options()): Options = {
		args match {
			case "--drop-db" :: tail => parseCommandLine(tail, options.copy(dropDb = true))
			case Nil => options
		}
	}
}

/**
	*
	* Create the main static method to run
	*/
object MasterNodeLauncher extends MasterNodeLauncherTrait