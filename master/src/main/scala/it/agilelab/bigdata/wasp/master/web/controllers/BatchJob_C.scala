package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages.StartBatchJob
import it.agilelab.bigdata.wasp.core.models.BatchJobModel
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object BatchJob_C extends Directives with JsonSupport {

  val logger = WaspLogger(BatchJob_C.getClass.getName)
  //TODO prendere il timeout dalla configurazione
  //implicit val timeout = Timeout(ConfigManager.config)
  implicit val timeout = Timeout(30, TimeUnit.SECONDS)

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("batchjobs") {
      pathEnd {
        get {
          complete {
            getJsonArrayOrEmpty[BatchJobModel](ConfigBL.batchJobBL.getAll, _.toJson)
          }
        } ~
          post {
            // unmarshal with in-scope unmarshaller
            entity(as[BatchJobModel]) { batchJobModel =>
              complete {
                ConfigBL.batchJobBL.insert(batchJobModel)
                "OK".toJson.toAngularOkResponse
              }
            }
          } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[BatchJobModel]) { batchJobModel =>
              complete {
                ConfigBL.batchJobBL.update(batchJobModel)
                "OK".toJson.toAngularOkResponse
              }
            }
          }
      } ~
        pathPrefix(Segment) { id =>
          path("start") {
            get {
              complete {
                masterGuardian ? StartBatchJob(id)
                "OK".toJson.toAngularOkResponse
              }

            }
          } ~
            pathEnd {
              get {
                complete {
                  getJsonOrNotFound[BatchJobModel](ConfigBL.batchJobBL.getById(id), id, "Batch job model", _.toJson)
                }

              } ~
                delete {
                  complete {
                    runIfExists(ConfigBL.batchJobBL.getById(id),
                      () => ConfigBL.batchJobBL.deleteById(id),
                      id,
                      "Machine learning model",
                      "delete")
                  }
                }
            }
        }

    }
  }
}
