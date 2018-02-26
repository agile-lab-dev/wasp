package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Configuration_C extends Directives with JsonSupport {
  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("configs") { parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
      path("kafka") {
        get {
          complete {
            // complete with serialized Future result
            ConfigManager.getKafkaConfig.toJson.toAngularOkResponse(pretty)
          }
        }
      } ~
        path("sparkbatch") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getSparkBatchConfig.toJson.toAngularOkResponse(pretty)
            }
          }
        } ~
        path("sparkstreaming") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getSparkStreamingConfig.toJson.toAngularOkResponse(pretty)
            }
          }
        } ~
        path("es") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getElasticConfig.toJson.toAngularOkResponse(pretty)
            }
          }
        } ~
        path("solr") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getSolrConfig.toJson.toAngularOkResponse(pretty)
            }
          }
        }
      }
    }
  }
}
