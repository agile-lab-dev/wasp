package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.wasp.core.models.FreeCodeModel
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.utils.{FreeCodeCompilerUtils, FreeCodeCompilerUtilsDefault}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._



class FreeCodeController(service : FreeCodeDBService = FreeCodeDBServiceDefault,
                         freeCodeCompilerUtils: FreeCodeCompilerUtils = FreeCodeCompilerUtilsDefault)
  extends Directives with JsonSupport {


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
              entity(as[FreeCodeModel]) { freeCodeModel =>
                complete {
                  val validationResult = freeCodeCompilerUtils.validate(freeCodeModel.code)
                  if (!validationResult.exists(_.errorType.equals("error"))) {
                    service.insert(freeCodeModel)
                    if(validationResult.isEmpty) "OK".toJson.toAngularOkResponse(pretty)
                    else validationResult.toJson.toAngularOkResponse(pretty)
                  }
                  else {
                    validationResult.toJson.toAngularKoResponse(s"FreeCodeStrategy with one or more problems",pretty)
                  }
                }
              }
            }
        } ~ pathPrefix(Segment) { name =>
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