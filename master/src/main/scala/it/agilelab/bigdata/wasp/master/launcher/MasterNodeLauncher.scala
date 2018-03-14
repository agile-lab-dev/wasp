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
import org.apache.commons.cli
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang.exception.ExceptionUtils

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
	* Launcher for the MasterGuardian and REST Server.
	* This trait is useful for who want extend the launcher
	* @author Nicolò Bidotti
	*/
trait MasterNodeLauncherTrait extends ClusterSingletonLauncher with WaspConfiguration {

	override def launch(commandLine: CommandLine): Unit = {

		if (commandLine.hasOption(MasterCommandLineOptions.dropDb.getOpt)) {
			// drop db
			val mongoDBConfig = ConfigManager.getMongoDBConfig

			logger.info(s"Dropping MongoDB database '${mongoDBConfig.databaseName}'")
			val mongoDBDatabase = MongoDBHelper.getDatabase(mongoDBConfig)
			val dropFuture = mongoDBDatabase.drop().toFuture()
			Await.result(dropFuture, Duration(10, TimeUnit.SECONDS))
			logger.info(s"Dropped MongoDB database '${mongoDBConfig.databaseName}'")
			System.exit(0)

			// re-initialize mongoDB and continue (instead of exit) -> not safe due to all process write on mongoDB
//			// db
//			WaspDB.initializeDB()
//			waspDB = WaspDB.getDB
//			// configs
//			ConfigManager.initializeCommonConfigs()
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
		//db.insertIfNotExists[TopicModel](SystemPipegraphs.rawTopic)
		//db.insertIfNotExists[IndexModel](SystemPipegraphs.rawIndex)
		//db.insertIfNotExists[PipegraphModel](SystemPipegraphs.rawPipegraph)
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
		logger.info(s"start rest server and bind on ${waspConfig.restServerHostname}:${waspConfig.restServerPort}")
		val bindingFuture = Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort)
	}
	
	override def getNodeName: String = "master"
	
	override def getOptions: Seq[cli.Option] = super.getOptions ++ MasterCommandLineOptions.allOptions
}

/**
	*
	* Create the main static method to run
	*/
object MasterNodeLauncher extends MasterNodeLauncherTrait