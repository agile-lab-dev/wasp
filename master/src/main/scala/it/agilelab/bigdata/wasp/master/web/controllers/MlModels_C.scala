package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._


/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object MlModels_C extends Directives with JsonSupport {
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
        path(Segment) { name =>
          path(Segment) { version =>
            get {
              complete {
                getJsonOrNotFound[MlModelOnlyInfo](ConfigBL.mlModelBL.getMlModelOnlyInfo(name,version), s"${name}/${version}", "Machine learning model", _.toJson)
              }
            } ~
              delete {
                complete {
                  val result = ConfigBL.mlModelBL.getMlModelOnlyInfo(name, version)


                  runIfExists(result,
                              () => ConfigBL.mlModelBL.delete(result.get.name, result.get.version, result.get.timestamp.getOrElse(0l)),
                              s"${name}/${version}", "Machine learning model", "delete")
                }
              }
          }
        }
    }
  }
}
