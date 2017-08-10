package it.agilelab.bigdata.wasp.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, PipegraphModel}
import it.agilelab.bigdata.wasp.web.controllers.Pipegraph_C.{as, complete, delete, entity, put}
import it.agilelab.bigdata.wasp.web.utils.JsonSupport
import spray.json._

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
            // complete with serialized Future result
            ConfigBL.mlModelBL.getAll.toJson
          }
        } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[MlModelOnlyInfo]) { mlModel =>
              complete {
                // complete with serialized Future result
                ConfigBL.mlModelBL.updateMlModelOnlyInfo(mlModel)
                "OK".toJson
              }
            }
          }
      } ~
        path(Segment) { id =>
          get {
            complete {
              // complete with serialized Future result
              ConfigBL.mlModelBL.getById(id).toJson
            }
          } ~
            delete {
              complete {
                // complete with serialized Future result
                ConfigBL.mlModelBL.delete(id).toJson
              }
            }
        }
    }
  }
}
