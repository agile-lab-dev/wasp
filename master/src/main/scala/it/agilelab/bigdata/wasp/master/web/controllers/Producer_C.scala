package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.messages.{StartProducer, StopProducer}
import it.agilelab.bigdata.wasp.core.models.ProducerModel
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
import akka.http.scaladsl.model.StatusCodes
import it.agilelab.bigdata.wasp.core.WaspSystem


/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Producer_C extends Directives with JsonSupport {
  implicit val implicitTimeout = WaspSystem.generalTimeout

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
                "OK".toJson.toAngularOkResponse
              }
            }
          }
      } ~
        path(Segment) { id =>
          get {
            complete {
              // complete with serialized Future result
              getJsonOrNotFound[ProducerModel](ConfigBL.producerBL.getById(id), id, "Producer model", _.toJson)
            }

          }

        } ~
        path(Segment / "start") { id =>
          post {
            complete {
              WaspSystem.??[Either[String, String]](masterGuardian, StartProducer(id)) match {
                case Right(s) => s.toJson.toAngularOkResponse
                case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
              }
            }
          }
        } ~
        path(Segment / "stop") { id =>
          post {
            complete {
              WaspSystem.??[Either[String, String]](masterGuardian, StopProducer(id)) match {
                case Right(s) => s.toJson.toAngularOkResponse
                case Left(s) => httpResponseJson(status = StatusCodes.InternalServerError, entity = angularErrorBuilder(s).toString)
              }
            }
          }
        }
    }
  }
}
