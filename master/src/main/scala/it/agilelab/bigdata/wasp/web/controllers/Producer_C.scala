package it.agilelab.bigdata.wasp.web.controllers

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages.{StartProducer, StopProducer}
import it.agilelab.bigdata.wasp.core.models.ProducerModel
import it.agilelab.bigdata.wasp.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import it.agilelab.bigdata.wasp.web.controllers.MlModels_C.logger

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object Producer_C extends Directives with JsonSupport {

  val logger = WaspLogger(Producer_C.getClass.getName)
  //implicit val timeout = Timeout(ConfigManager.config)
  implicit val timeout = Timeout(30, TimeUnit.SECONDS)

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
              // complete with serialized Future result
              WaspSystem.masterActor ? StartProducer(id)
              "OK".toJson.toAngularOkResponse
            }
          }
        } ~
        path(Segment / "stop") { id =>
          post {
            complete {
              // complete with serialized Future result
              WaspSystem.masterActor ? StopProducer(id)
              "OK".toJson.toAngularOkResponse
            }
          }
        }
    }
  }
}
