package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.StartBatchJob
import it.agilelab.bigdata.wasp.core.models.{BatchJobInstanceModel, BatchJobModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object BatchJob_C extends Directives with JsonSupport {

  def getRoute: Route = {
    pathPrefix("batchjobs") {
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              getJsonArrayOrEmpty[BatchJobModel](ConfigBL.batchJobBL.getAll, _.toJson, pretty)
            }
          } ~
            post {
              // unmarshal with in-scope unmarshaller
              entity(as[BatchJobModel]) { batchJobModel =>
                complete {
                  ConfigBL.batchJobBL.insert(batchJobModel)
                  "OK".toJson.toAngularOkResponse(pretty)
                }
              }
            } ~
            put {
              // unmarshal with in-scope unmarshaller
              entity(as[BatchJobModel]) { batchJobModel =>
                complete {
                  ConfigBL.batchJobBL.update(batchJobModel)
                  "OK".toJson.toAngularOkResponse(pretty)
                }
              }
            }
        } ~
          pathPrefix(Segment) { name =>
            path("start") {
              post {
                entity(as[Config]) { restConfig =>
                  complete {
                    WaspSystem.??[Either[String, String]](masterGuardian, StartBatchJob(name, restConfig)) match {
                      case Right(s) => s.toJson.toAngularOkResponse(pretty)
                      case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                    }
                  }
                } ~
                  complete {
                    WaspSystem.??[Either[String, String]](masterGuardian, StartBatchJob(name, ConfigFactory.empty)) match {
                      case Right(s) => s.toJson.toAngularOkResponse(pretty)
                      case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                    }
                  }
              }
            } ~
              pathPrefix("instances") {
                pathPrefix(Segment) { instanceName =>
                  get {
                    complete {
                      val instance = ConfigBL.batchJobBL.instances().getByName(instanceName)
                      if ((instance.isDefined) && (instance.get.instanceOf != name))
                        httpResponseJson(
                          entity = JsonResultsHelper.angularErrorBuilder(s"Batch job instance '$instanceName' not related to batch job '$name'").toString(),
                          status = StatusCodes.BadRequest
                        )
                      else
                        getJsonOrNotFound[BatchJobInstanceModel](instance, name, "Batch job instance", _.toJson, pretty)
                    }
                  }
                } ~
                  pathEnd {
                    get {
                      complete {
                        getJsonArrayOrEmpty[BatchJobInstanceModel](ConfigBL.batchJobBL.instances().instancesOf(name).sortBy { instance => -instance.startTimestamp }, _.toJson, pretty)
                      }
                    }
                  }
              } ~
              pathEnd {
                get {
                  complete {
                    getJsonOrNotFound[BatchJobModel](ConfigBL.batchJobBL.getByName(name), name, "Batch job model", _.toJson, pretty)
                  }
                } ~
                  delete {
                    complete {
                      val batchJob = ConfigBL.batchJobBL.getByName(name)
                      runIfExists(batchJob, () => ConfigBL.batchJobBL.deleteByName(batchJob.get.name), name, "Batch job model", "delete", pretty)
                    }
                  }
              }
          }
      }
    }
  }
}