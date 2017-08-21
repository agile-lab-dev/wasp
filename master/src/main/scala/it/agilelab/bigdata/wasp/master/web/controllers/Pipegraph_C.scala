package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.{Directives, Route}
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.{StartPipegraph, StopPipegraph}
import it.agilelab.bigdata.wasp.core.models.PipegraphModel
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
import akka.http.scaladsl.model.StatusCodes
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian


/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Pipegraph_C extends Directives with JsonSupport {
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
                  WaspSystem.??[Either[String, String]](masterGuardian, StartPipegraph(id)) match {
                    case Right(s) => s.toJson.toAngularOkResponse
                    case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                  }
                }
              }
            } ~
            path("stop") {
              post {
                complete {
                  // complete with serialized Future result
                  WaspSystem.??[Either[String, String]](masterGuardian, StopPipegraph(id)) match {
                    case Right(s) => s.toJson.toAngularOkResponse
                    case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                  }
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