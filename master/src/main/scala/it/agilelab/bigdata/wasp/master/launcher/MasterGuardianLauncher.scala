package it.agilelab.bigdata.wasp.master.launcher

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives.{complete, extractUri, handleExceptions, _}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration
import it.agilelab.bigdata.wasp.master.MasterGuardian
import it.agilelab.bigdata.wasp.master.web.controllers._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper
import org.apache.commons.lang3.exception.ExceptionUtils

/**
	* Launcher for the MasterGuardian and REST API.
	* @author Nicolò Bidotti
	*/
object MasterGuardianLauncher extends ClusterSingletonLauncher with WaspConfiguration {
	private val logger = WaspLogger(getClass.getName)
	
	override def launch(args: Array[String]): Unit = {
		// launch cluster singleton
		super.launch(args)
		
		// launch rest server
		startRestServer(WaspSystem.actorSystem, getRoutes)
	}
	
	override def getSingletonProps: Props = {
		Props(new MasterGuardian(ConfigBL))
	}
	
	override def getSingletonName: String = MasterGuardian.name
	
	override def getSingletonRole: String = MasterGuardian.role
	
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
		Topic_C.getRoute
	}
	
	private def startRestServer(actorSystem: ActorSystem, route: Route): Unit = {
		implicit val system = actorSystem
		implicit val materializer = ActorMaterializer()
		val finalRoute = handleExceptions(myExceptionHandler)(route)
		val bindingFuture = Http().bindAndHandle(finalRoute, waspConfig.restServerHostname, waspConfig.restServerPort)
	}
}