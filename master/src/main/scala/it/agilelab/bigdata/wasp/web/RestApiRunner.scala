package it.agilelab.bigdata.wasp.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.actorSystem
import it.agilelab.bigdata.wasp.core.launcher.WaspLauncher
import it.agilelab.bigdata.wasp.web.controllers.{Index_C, Pipegraph_C, Topic_C}

/**
  * Created by Agile Lab s.r.l. on 04/08/2017.
  */
class RestApiRunner {



  def start(actorSystem: ActorSystem, route: Route) = {
    implicit val system = actorSystem
    implicit val materializer = ActorMaterializer()
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  }
}

object RestApiRunner {
  def main(args: Array[String]): Unit = {


  }
}

object RestApiRunnerLauncher extends WaspLauncher {

  override protected def startApp(args: Array[String]): Unit = {
    new RestApiRunner().start(WaspSystem.actorSystem,  Index_C.getRoute ~ Topic_C.getRoute ~ Pipegraph_C.getRoute)
  }
  /**
    * Launchers must override this with deployment-specific pipegraph initialization logic;
    * this usually simply means loading the custom pipegraphs into the database.
    */
  override def initializeCustomWorkloads(): Unit = {}
}