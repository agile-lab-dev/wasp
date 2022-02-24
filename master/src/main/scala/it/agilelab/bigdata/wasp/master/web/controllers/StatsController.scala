package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import it.agilelab.bigdata.wasp.utils.JsonSupport

import java.time.Instant

class StatsController(logs: StatsService) extends Directives with JsonSupport {

  val parseInstant: Unmarshaller[String, Instant] =
    Unmarshaller.identityUnmarshaller[String].map(x => Instant.parse(x))

  def getRoutes: Route = pretty(stats)

  def pretty(subroute: Boolean => Route): Route =
    parameters('pretty.as[Boolean].?(false)) { pretty =>
      subroute(pretty)
    }

  def stats(pretty: Boolean): Route = get {
    path("stats") {
      parameter('startTimestamp.as[Instant](parseInstant)) { startTimestamp =>
        parameter('endTimestamp.as[Instant](parseInstant)) { endTimestamp =>
          parameter('size.as[Int]) { size =>
            extractExecutionContext { implicit ec =>
              complete {
                import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
                import spray.json._
                logs.counts(startTimestamp, endTimestamp, size).map(_.toJson.toAngularOkResponse(pretty))
              }
            }
          }
        }
      }
    }
  }
}
