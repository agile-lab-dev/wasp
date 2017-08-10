package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages.{StartPipegraph, StopPipegraph}
import it.agilelab.bigdata.wasp.core.models.PipegraphModel
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object Pipegraph_C extends Directives with JsonSupport {

  val logger = WaspLogger(Pipegraph_C.getClass.getName)
  //TODO prendere il timeout dalla configurazione
  //implicit val timeout = Timeout(ConfigManager.config)
  implicit val timeout = Timeout(30, TimeUnit.SECONDS)


  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("pipegraphs") {
      pathEnd {
        get {
          complete {
            getJsonArrayOrEmpty[PipegraphModel](ConfigBL.pipegraphBL.getAll, _.toJson)
          }
        } ~
          post {
            // unmarshal with in-scope unmarshaller
            entity(as[PipegraphModel]) { pipegraph =>
              complete {
                // complete with serialized Future result
                ConfigBL.pipegraphBL.insert(pipegraph)
                "OK".toJson.toAngularOkResponse
              }
            }
          } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[PipegraphModel]) { pipegraph =>
              complete {
                // complete with serialized Future result
                ConfigBL.pipegraphBL.update(pipegraph)
                "OK".toJson.toAngularOkResponse
              }
            }
          }
      } ~
        pathPrefix(Segment) { id =>
            path("start") {
              post {
                complete {
                  // complete with serialized Future result
                  WaspSystem.masterActor ? StartPipegraph(id)
                  "OK".toJson.toAngularOkResponse
                }
              }
            } ~
            path("stop") {
              post {
                complete {
                  // complete with serialized Future result
                  WaspSystem.masterActor ? StopPipegraph(id)
                  "OK".toJson.toAngularOkResponse
                }
              }
            } ~
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                getJsonOrNotFound[PipegraphModel](ConfigBL.pipegraphBL.getById(id), id, "Pipegraph", _.toJson)
              }
            } ~
              delete {
                complete {
                  // complete with serialized Future result
                  val result = ConfigBL.pipegraphBL.getById(id)
                  runIfExists(result, () => ConfigBL.pipegraphBL.deleteById(id), id, "Pipegraph", "delete")
                }

              }
          }
        } ~
        path("name" / Segment) { name: String =>
          get {
            complete {
              getJsonOrNotFound[PipegraphModel](ConfigBL.pipegraphBL.getByName(name), name, "Pipegraph", _.toJson)
            }

          }
        }
    }
  }
}