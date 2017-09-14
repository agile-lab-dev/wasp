package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.StartBatchJob
import it.agilelab.bigdata.wasp.core.models.BatchJobModel
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._


/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object BatchJob_C extends Directives with JsonSupport {
  implicit val implicitTimeout = WaspSystem.generalTimeout

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
        pathPrefix(Segment) { name =>
          path("start") {
            get {
              complete {
                WaspSystem.??[Either[String, String]](masterGuardian, StartBatchJob(name)) match {
                  case Right(s) => s.toJson.toAngularOkResponse
                  case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                }
              }

            }
          } ~
            pathEnd {
              get {
                complete {
                  getJsonOrNotFound[BatchJobModel](ConfigBL.batchJobBL.getByName(name), name, "Batch job model", _.toJson)
                }

              } ~
                delete {
                  complete {
                    val batchJob = ConfigBL.batchJobBL.getByName(name)
                    runIfExists(batchJob,
                      () => ConfigBL.batchJobBL.deleteById(batchJob.get._id.get.asObjectId().getValue.toHexString),
                      name,
                      "Machine learning model",
                      "delete")
                  }
                }
            }
        }

    }
  }
}
