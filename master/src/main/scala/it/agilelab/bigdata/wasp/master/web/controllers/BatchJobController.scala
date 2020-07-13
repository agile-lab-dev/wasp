package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, BatchJobModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
class BatchJobController(batchJobService: BatchJobService)
  extends Directives
    with JsonSupport {

  def pretty(subroute: Boolean => Route): Route =
    parameters('pretty.as[Boolean].?(false)) { pretty =>
      subroute(pretty)
    }

  def listRoute(pretty: Boolean): Route = get {
    path("batchjobs") {
      complete {
        batchJobService.list().toJson.toAngularOkResponse(pretty)
      }
    }
  }

  def insertRoute(pretty: Boolean): Route = post {
    path("batchjobs") {
      entity(as[BatchJobModel]) { batchJob =>
        complete {
          batchJobService.insert(batchJob)
          "OK".toJson.toAngularOkResponse(pretty)
        }
      }
    }
  }

  def updateRoute(pretty: Boolean): Route = put {
    path("batchjobs") {
      entity(as[BatchJobModel]) { batchJob =>
        complete {
          batchJobService.update(batchJob)
          "OK".toJson.toAngularOkResponse(pretty)
        }
      }
    }
  }

  def startRoute(pretty: Boolean): Route = {

    def action(name: String, restConfig: Config) = complete {
      batchJobService.start(name, restConfig) match {
        case Right(jsonToParse) =>
          jsonToParse.parseJson.toAngularOkResponse(pretty)
        case Left(s) =>
          httpResponseJson(
            status = StatusCodes.InternalServerError,
            entity = angularErrorBuilder(s).toString
          )

      }
    }

    post {
      path("batchjobs" / Segment / "start") { name =>
        entity(as[Config])(action(name, _)) ~ action(
          name,
          ConfigFactory.empty()
        )
      }
    }
  }

  def instancesRoute(pretty: Boolean): Route = {
    get {
      path("batchjobs" / Segment / "instances" ) { name =>
        complete {
          getJsonArrayOrEmpty[BatchJobInstanceModel](
            batchJobService.instanceOf(name),
            _.toJson,
            pretty
          )
        }
      }
    }
  }

  def instanceRoute(pretty: Boolean): Route = {
    get {
      path("batchjobs" / Segment / "instances" / Segment) {
        (name, instanceName) =>
          complete {
            val instance: Option[BatchJobInstanceModel] =
              batchJobService.instance(instanceName)
            if ((instance.isDefined) && (instance.get.instanceOf != name))
              httpResponseJson(
                entity = JsonResultsHelper
                  .angularErrorBuilder(
                    s"Batch job instance '$instanceName' not related to batch job '$name'"
                  )
                  .toString(),
                status = StatusCodes.BadRequest
              )
            else
              getJsonOrNotFound[BatchJobInstanceModel](
                instance,
                name,
                "Batch job instance",
                _.toJson,
                pretty
              )
          }
      }
    }
  }

  def deleteRoute(pretty: Boolean): Route = {
    delete {
      path("batchjobs" / Segment ) { name =>
        complete {
          val batchJob = batchJobService.get(name)
          if (batchJob.isDefined) {
            batchJobService.delete(batchJob.get)
            "OK".toJson.toAngularOkResponse(pretty)
          } else {
            val msg =
              s"Batch job model '$name' not found isn't possible delete"
            httpResponseJson(
              entity = JsonResultsHelper
                .angularErrorBuilder(msg)
                .toJson
                .toString(),
              status = StatusCodes.NotFound
            )
          }
        }
      }
    }
  }


  def newStyleRoute : Route = pretty{ isPretty =>
    List(listRoute _, insertRoute _, updateRoute _ , startRoute _, instancesRoute _, instanceRoute _ , deleteRoute _)
      .map(route => route(isPretty))
      .reduce(_ ~ _)
  }

  def getRoute: Route = {
    pathPrefix("batchjobs") {
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {

              getJsonArrayOrEmpty[BatchJobModel](
                batchJobService.list(),
                _.toJson,
                pretty
              )
            }
          } ~
            post {
              // unmarshal with in-scope unmarshaller
              entity(as[BatchJobModel]) { batchJobModel =>
                complete {
                  batchJobService.insert(batchJobModel)
                  "OK".toJson.toAngularOkResponse(pretty)
                }
              }
            } ~
            put {
              // unmarshal with in-scope unmarshaller
              entity(as[BatchJobModel]) { batchJobModel =>
                complete {
                  batchJobService.update(batchJobModel)
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
                    batchJobService.start(name, restConfig) match {
                      case Right(jsonToParse) =>
                        jsonToParse.parseJson.toAngularOkResponse(pretty)
                      case Left(s) =>
                        httpResponseJson(
                          status = StatusCodes.InternalServerError,
                          entity = angularErrorBuilder(s).toString
                        )

                    }
                  }
                }
              } ~
                complete {
                  batchJobService.start(name, ConfigFactory.empty()) match {
                    case Right(jsonToParse) =>
                      jsonToParse.parseJson.toAngularOkResponse(pretty)
                    case Left(s) =>
                      httpResponseJson(
                        status = StatusCodes.InternalServerError,
                        entity = angularErrorBuilder(s).toString
                      )
                  }
                }
            } ~
              pathPrefix("instances") {
                pathPrefix(Segment) { instanceName =>
                  get {
                    complete {
                      val instance: Option[BatchJobInstanceModel] =
                        batchJobService.instance(instanceName)
                      if ((instance.isDefined) && (instance.get.instanceOf != name))
                        httpResponseJson(
                          entity = JsonResultsHelper
                            .angularErrorBuilder(
                              s"Batch job instance '$instanceName' not related to batch job '$name'"
                            )
                            .toString(),
                          status = StatusCodes.BadRequest
                        )
                      else
                        getJsonOrNotFound[BatchJobInstanceModel](
                          instance,
                          name,
                          "Batch job instance",
                          _.toJson,
                          pretty
                        )
                    }
                  }
                } ~
                  pathEnd {
                    get {
                      complete {
                        getJsonArrayOrEmpty[BatchJobInstanceModel](
                          batchJobService.instanceOf(name),
                          _.toJson,
                          pretty
                        )
                      }
                    }
                  }
              } ~
              pathEnd {
                get {
                  complete {
                    getJsonOrNotFound[BatchJobModel](
                      batchJobService.get(name),
                      name,
                      "Batch job model",
                      _.toJson,
                      pretty
                    )
                  }
                } ~
                  delete {
                    complete {
                      val batchJob = batchJobService.get(name)
                      if (batchJob.isDefined) {
                        batchJobService.delete(batchJob.get)
                        "OK".toJson.toAngularOkResponse(pretty)
                      } else {
                        val msg =
                          s"Batch job model '$name' not found isn't possible delete"
                        httpResponseJson(
                          entity = JsonResultsHelper
                            .angularErrorBuilder(msg)
                            .toJson
                            .toString(),
                          status = StatusCodes.NotFound
                        )
                      }
                    }
                  }
              }
          }
      }
    }
  }

}