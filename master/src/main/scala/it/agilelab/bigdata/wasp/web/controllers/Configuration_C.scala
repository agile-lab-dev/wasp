package it.agilelab.bigdata.wasp.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.web.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object Configuration_C extends Directives with JsonSupport {

  val logger = WaspLogger(Configuration_C.getClass.getName)

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("configs") {
      path("kafka") {
        get {
          complete {
            // complete with serialized Future result
            ConfigManager.getKafkaConfig.toJson
          }
        }
      } ~
        path("sparkbatch") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getSparkBatchConfig.toJson
            }
          }
        } ~
        path("sparkstreaming") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getSparkStreamingConfig.toJson
            }
          }
        } ~
        path("es") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getElasticConfig.toJson
            }
          }
        } ~
        path("solr") {
          get {
            complete {
              // complete with serialized Future result
              ConfigManager.getSolrConfig.toJson
            }
          }
        }
    }
  }
}
