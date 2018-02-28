package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.StartBatchJob
import it.agilelab.bigdata.wasp.core.models.{BatchJobInstanceModel, BatchJobModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object BatchJob_C extends Directives with JsonSupport {

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
            post {
              complete {
                WaspSystem.??[Either[String, String]](masterGuardian, StartBatchJob(name)) match {
                  case Right(s) => s.toJson.toAngularOkResponse
                  case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                }
              }

            }
          } ~
          path("instances") {
            get {
              complete {
                getJsonArrayOrEmpty[BatchJobInstanceModel](ConfigBL.batchJobBL.instances().instancesOf(name).sortBy{ instance => -instance.startTimestamp }, _.toJson)
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
                      () => ConfigBL.batchJobBL.deleteByName(batchJob.get.name),
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