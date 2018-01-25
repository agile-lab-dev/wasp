package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.{StartPipegraph, StopPipegraph}
import it.agilelab.bigdata.wasp.core.models.PipegraphModel
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._


/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Pipegraph_C extends Directives with JsonSupport {

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
        pathPrefix(Segment) { name =>
          path("start") {
              post {
                complete {
                  WaspSystem.??[Either[String, String]](masterGuardian, StartPipegraph(name)) match {
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
                  WaspSystem.??[Either[String, String]](masterGuardian, StopPipegraph(name)) match {
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
                  getJsonOrNotFound[PipegraphModel](ConfigBL.pipegraphBL.getByName(name), name, "Pipegraph", _.toJson)
                }
              } ~
                delete {
                  complete {
                    // complete with serialized Future result
                    val result = ConfigBL.pipegraphBL.getByName(name)
                    runIfExists(result, () => ConfigBL.pipegraphBL.deleteById(name), name, "Pipegraph", "delete")
                  }

                }
            }
        }
    }
  }
}