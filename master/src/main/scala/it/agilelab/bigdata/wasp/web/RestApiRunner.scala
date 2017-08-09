package it.agilelab.bigdata.wasp.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.launcher.WaspLauncher
import it.agilelab.bigdata.wasp.web.controllers.Index_C

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

  override def launch(args: Array[String]): Unit = {
    new RestApiRunner().start(ActorSystem("test"),  Index_C.getRoute)
  }
}