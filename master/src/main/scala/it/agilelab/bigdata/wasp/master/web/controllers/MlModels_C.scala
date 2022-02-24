package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object MlModels_C extends Directives with JsonSupport {

  def pretty(route: Boolean => Route) = parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
    route(pretty)
  }

  def list(pretty: Boolean): Route = path("mlmodels") {
    get {
      complete {
        val result = ConfigBL.mlModelBL.getAll
        getJsonArrayOrEmpty[MlModelOnlyInfo](
          ConfigBL.mlModelBL.getAll,
          _.toJson,
          pretty
        )
        val finalResult: JsValue = if (result.isEmpty) {
          JsArray()
        } else {
          result.toJson
        }

        finalResult.toAngularOkResponse(pretty)
      }
    }
  }

  def update(pretty: Boolean): Route = path("mlmodels") {
    put {
      entity(as[MlModelOnlyInfo]) { mlModel =>
        complete {

          ConfigBL.mlModelBL.updateMlModelOnlyInfo(mlModel)
          "OK".toJson
        }
      }
    }
  }

  def insert(pretty: Boolean) = path("mlmodels" ) {
    post { // unmarshal with in-scope unmarshaller
      entity(as[MlModelOnlyInfo]) { mlModel =>
        complete {

          ConfigBL.mlModelBL.saveMlModelOnlyInfo(mlModel)
          "OK".toJson
        }
      }
    }
  }

  def instance(pretty: Boolean): Route = path("mlmodels" / Segment / Segment) { (name, version) =>
    get {
      complete {
        getJsonOrNotFound[MlModelOnlyInfo](
          ConfigBL.mlModelBL.getMlModelOnlyInfo(name, version),
          s"${name}/${version}",
          "Machine learning model",
          _.toJson,
          pretty
        )
      }
    }
  }

  def deleteInstance(pretty: Boolean): Route = path("mlmodels" / Segment / Segment) { (name, version) =>
    delete {
      complete {
        val result =
          ConfigBL.mlModelBL.getMlModelOnlyInfo(name, version)
        runIfExists(
          result,
          () =>
            ConfigBL.mlModelBL.delete(
              result.get.name,
              result.get.version,
              result.get.timestamp.getOrElse(0L)
            ),
          s"${name}/${version}",
          "Machine learning model",
          "delete",
          pretty
        )
      }
    }
  }

  def getRoute: Route = pretty { pretty =>
    list(pretty) ~ update(pretty) ~ insert(pretty) ~ instance(pretty) ~ deleteInstance(pretty)
  }
}
