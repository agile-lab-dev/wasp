package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import it.agilelab.bigdata.wasp.master.web.controllers.Pipegraph_C.logger

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object MlModels_C extends Directives with JsonSupport {

  val logger = WaspLogger(MlModels_C.getClass.getName)

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("mlmodels") {
      pathEnd {
        get {
          complete {
            
            val result = ConfigBL.mlModelBL.getAll
            getJsonArrayOrEmpty[MlModelOnlyInfo](ConfigBL.mlModelBL.getAll, _.toJson)
            val finalResult: JsValue = if (result.isEmpty) {
              JsArray()
            } else {
              result.toJson
            }

            finalResult.toAngularOkResponse
          }
        } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[MlModelOnlyInfo]) { mlModel =>
              complete {
                
                ConfigBL.mlModelBL.updateMlModelOnlyInfo(mlModel)
                "OK".toJson
              }
            }
          }
      } ~
        path(Segment) { id =>
          get {
            complete {
              getJsonOrNotFound[MlModelOnlyInfo](ConfigBL.mlModelBL.getById(id), id, "Machine learning model", _.toJson)
            }
          } ~
            delete {
              complete {
                val result = ConfigBL.mlModelBL.getById(id)
                runIfExists(result, () => ConfigBL.mlModelBL.delete(id), id, "Machine learning model", "delete")
              }
            }
        }
    }
  }
}
