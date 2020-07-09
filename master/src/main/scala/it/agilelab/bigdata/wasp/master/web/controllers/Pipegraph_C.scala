package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.{StartPipegraph, StopPipegraph}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel}
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Pipegraph_C extends Directives with JsonSupport {

  def getRoute: Route = {
    pathPrefix("pipegraphs") {
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              getJsonArrayOrEmpty[PipegraphModel](ConfigBL.pipegraphBL.getAll, _.toJson, pretty)
            }
          } ~
            post {
              // unmarshal with in-scope unmarshaller
              entity(as[PipegraphModel]) { pipegraph =>
                complete {
                  // complete with serialized Future result
                  ConfigBL.pipegraphBL.insert(pipegraph)
                  "OK".toJson.toAngularOkResponse(pretty)
                }
              }
            } ~
            put {
              // unmarshal with in-scope unmarshaller
              entity(as[PipegraphModel]) { pipegraph =>
                complete {
                  // complete with serialized Future result
                  ConfigBL.pipegraphBL.update(pipegraph)
                  "OK".toJson.toAngularOkResponse(pretty)
                }
              }
            }
        } ~
          pathPrefix(Segment) { name =>
            path("start") {
              post {
                complete {
                  WaspSystem.??[Either[String, String]](masterGuardian, StartPipegraph(name)) match {
                    case Right(jsonToParse) => jsonToParse.parseJson.toAngularOkResponse(pretty)
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
                      case Right(s) => s.toJson.toAngularOkResponse(pretty)
                      case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                    }
                  }
                }
              } ~
              pathPrefix("instances") {
                pathPrefix(Segment) { instanceName =>
                  get {
                    complete {
                      val instance = ConfigBL.pipegraphBL.instances().getByName(instanceName)
                      if ((instance.isDefined) && (instance.get.instanceOf != name))
                        httpResponseJson(
                          entity = JsonResultsHelper.angularErrorBuilder(s"Pipegraph instance '$instanceName' not related to pipegraph '$name'").toString(),
                          status = StatusCodes.BadRequest
                        )
                      else
                        getJsonOrNotFound[PipegraphInstanceModel](instance, name, "Pipegraphinstance", _.toJson, pretty)
                    }
                  }
                } ~
                pathEnd {
                  get {
                    complete {
                      getJsonArrayOrEmpty[PipegraphInstanceModel](ConfigBL.pipegraphBL.instances().instancesOf(name).sortBy { instance => -instance.startTimestamp }, _.toJson, pretty)
                    }
                  }
                }
              } ~
              pathEnd {
                get {
                  complete {
                    // complete with serialized Future result
                    getJsonOrNotFound[PipegraphModel](ConfigBL.pipegraphBL.getByName(name), name, "Pipegraph model", _.toJson, pretty)
                  }
                } ~
                  delete {
                    complete {
                      // complete with serialized Future result
                      val result = ConfigBL.pipegraphBL.getByName(name)
                      runIfExists(result, () => ConfigBL.pipegraphBL.deleteByName(name), name, "Pipegraph model", "delete", pretty)
                    }
                  }
              }
          }
      }
    }
  }
}