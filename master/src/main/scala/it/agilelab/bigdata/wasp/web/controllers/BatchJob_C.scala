package it.agilelab.bigdata.wasp.web.controllers

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages.StartBatchJob
import it.agilelab.bigdata.wasp.core.models.BatchJobModel
import it.agilelab.bigdata.wasp.web.utils.JsonSupport
import spray.json._

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
            // complete with serialized Future result
            ConfigBL.batchJobBL.getAll.toJson
          }
        } ~
          post {
            // unmarshal with in-scope unmarshaller
            entity(as[BatchJobModel]) { batchJobModel =>
              complete {
                // complete with serialized Future result
                ConfigBL.batchJobBL.insert(batchJobModel)
                "OK".toJson
              }
            }
          } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[BatchJobModel]) { batchJobModel =>
              complete {
                // complete with serialized Future result
                ConfigBL.batchJobBL.update(batchJobModel)
                "OK".toJson
              }
            }
          }
      } ~
        pathPrefix(Segment) { id =>
          path("start") {
            get {
              complete {
                // complete with serialized Future result
                WaspSystem.masterActor ? StartBatchJob(id)
                "OK".toJson
              }

            }
          } ~
            pathEnd {
              get {
                complete {
                  // complete with serialized Future result
                  ConfigBL.batchJobBL.getById(id).toJson
                }

              } ~
                delete {
                  complete {
                    // complete with serialized Future result
                    ConfigBL.batchJobBL.deleteById(id)
                    "OK".toJson
                  }

                }
            }
        }

    }
  }
}
