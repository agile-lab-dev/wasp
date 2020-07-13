package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.utils.FreeCodeCompilerUtils
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.models.{FreeCode, FreeCodeModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

class FreeCodeController(service: FreeCodeDBService, freeCodeCompilerUtils: FreeCodeCompilerUtils)
    extends Directives
    with JsonSupport {

  def getRoute: Route = {
    pathPrefix("freeCode") {
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              getJsonArrayOrEmpty[FreeCodeModel](service.getAll, _.toJson, pretty)
            }
          } ~
            post {
              // unmarshal with in-scope unmarshaller
              entity(as[FreeCodeModel]) { freeCode =>
                complete {
                  val validationResult = freeCodeCompilerUtils.validate(freeCode.code)
                  if (!validationResult.exists(_.errorType.equals("error"))) {
                    service.insert(freeCode)
                    if (validationResult.isEmpty) "OK".toJson.toAngularOkResponse(pretty)
                    else validationResult.toJson.toAngularOkResponse(pretty)
                  } else {
                    validationResult.toJson.toAngularKoResponse(s"FreeCodeStrategy with one or more problems", pretty)
                  }
                }
              }
            }
        } ~
          pathPrefix("validate") {
            pathEnd {
              post {
                // unmarshal with in-scope unmarshaller
                entity(as[FreeCode]) { freeCode =>
                  complete {
                    val validationResult = freeCodeCompilerUtils.validate(freeCode.code)
                    if (!validationResult.exists(_.errorType.equals("error"))) {
                      if (validationResult.isEmpty) "OK".toJson.toAngularOkResponse(pretty)
                      else validationResult.toJson.toAngularOkResponse(pretty)
                    } else {
                      validationResult.toJson.toAngularKoResponse(s"FreeCodeStrategy with one or more problems", pretty)
                    }
                  }
                }
              }
            }
          } ~
          pathPrefix("complete") {
            pathPrefix(Segment) { position =>
              pathEnd {
                post {
                  entity(as[FreeCode]) { freeCode =>
                    complete {
                      val completionResult = freeCodeCompilerUtils.complete(freeCode.code, position.toInt)
                      completionResult.toJson.toAngularOkResponse(pretty)
                    }
                  }
                }
              }
            }
          } ~
          pathPrefix("instance") {
            pathPrefix(Segment) { name =>
              pathEnd {
                get {
                  complete {
                    // complete with serialized Future result
                    getJsonOrNotFound[FreeCodeModel](service.getByName(name), name, "FreeCode model", _.toJson, pretty)
                  }
                } ~
                  delete {
                    complete {
                      // complete with serialized Future result
                      val result = service.getByName(name)
                      runIfExists(result, () => service.deleteByName(name), name, "FreeCode model", "delete", pretty)
                    }
                  }
              }
            }
          }
      }
    }
  }

}
