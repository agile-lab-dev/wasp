package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.model.{HttpMethod, StatusCodes}
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.{ModelKey, RestProducerRequest, StartProducer, StopProducer}
import it.agilelab.bigdata.wasp.core.models.ProducerModel
import it.agilelab.bigdata.wasp.master.web.models.RestProducerModel
import it.agilelab.bigdata.wasp.master.web.models.RestProducerModelJsonProtocol._
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

import scala.collection.immutable

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */

object Producer_C extends Directives with JsonSupport {

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("producers") {
      pathEnd {
        get {
          complete {
            getJsonArrayOrEmpty[ProducerModel](ConfigBL.producerBL.getAll, _.toJson)
          }
        } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[ProducerModel]) { producerModel =>
              complete {
                // complete with serialized Future result
                ConfigBL.producerBL.update(producerModel)
                "OK".toJson.toAngularOkResponse()
              }
            }
          }
      } ~
        path(Segment) { name =>
          get {
            complete {
              // complete with serialized Future result
              getJsonOrNotFound[ProducerModel](ConfigBL.producerBL.getByName(name), name, "Producer model", _.toJson)
            }

          }

        } ~
        path(Segment / "start") { name =>
          post {
            complete {
              WaspSystem.??[Either[String, String]](masterGuardian, StartProducer(name)) match {
                case Right(s) => s.toJson.toAngularOkResponse()
                case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
              }
            }
          }
        } ~
        path(Segment / "stop") { name =>
          post {
            complete {
              WaspSystem.??[Either[String, String]](masterGuardian, StopProducer(name)) match {
                case Right(s) => s.toJson.toAngularOkResponse()
                case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
              }
            }
          }
        } ~
        path(Segment / "rest") { name =>
          post {
            decodeRequest {
              entity(as[JsValue]) { json: JsValue =>
                complete {
                  val request = json.convertTo[RestProducerModel]
                  val httpMethod = HttpMethod.custom(request.httpMethod)
                  val data = request.data

                  ModelKey
                  WaspSystem.??[Either[String, String]](masterGuardian, RestProducerRequest(name, httpMethod, data, request.mlModel)) match {
                    case Right(s) => s.toJson.toAngularOkResponse()
                    case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
                  }
                }
              }
            }
          }
        }
    }
  }
}
