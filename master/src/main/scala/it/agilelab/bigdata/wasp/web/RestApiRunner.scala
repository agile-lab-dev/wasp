package it.agilelab.bigdata.wasp.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.launcher.WaspLauncher
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.web.controllers._
import it.agilelab.bigdata.wasp.web.utils.JsonResultsHelper
import org.apache.commons.lang3.exception.ExceptionUtils
/**
  * Created by Agile Lab s.r.l. on 04/08/2017.
  */
class RestApiRunner {
  val logger = WaspLogger(getClass.getName)

  val myExceptionHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        val resultJson = JsonResultsHelper.angularErrorBuilder(ExceptionUtils.getStackTrace(e)).toString()
        logger.error(s"Request to $uri could not be handled normally, result: $resultJson", e)
        complete(HttpResponse(InternalServerError, entity = resultJson))
      }
  }


  def start(actorSystem: ActorSystem, route: Route) = {
    implicit val system = actorSystem
    implicit val materializer = ActorMaterializer()
    val finalRoute = handleExceptions(myExceptionHandler)(route)
    val bindingFuture = Http().bindAndHandle(finalRoute, "localhost", 8080)

  }
}


object RestApiRunnerLauncher extends WaspLauncher {

  override protected def startApp(args: Array[String]): Unit = {
    new RestApiRunner().start(WaspSystem.actorSystem,
      BatchJob_C.getRoute ~ Configuration_C.getRoute ~
        Index_C.getRoute ~ MlModels_C.getRoute  ~
        Pipegraph_C.getRoute ~ Producer_C.getRoute ~
        Topic_C.getRoute
    )
  }
  /**
    * Launchers must override this with deployment-specific pipegraph initialization logic;
    * this usually simply means loading the custom pipegraphs into the database.
    */
  override def initializeCustomWorkloads(): Unit = {}
}