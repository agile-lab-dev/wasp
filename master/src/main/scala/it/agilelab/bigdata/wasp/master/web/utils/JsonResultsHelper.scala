package it.agilelab.bigdata.wasp.master.web.utils

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.master.web.controllers.Pipegraph_C
import it.agilelab.bigdata.wasp.master.web.controllers.Pipegraph_C.logger
import it.agilelab.bigdata.wasp.master.web.controllers.Producer_C.logger
import org.apache.commons.lang3.exception.ExceptionUtils
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.{Await, ExecutionContext}
import spray.json._

import scala.collection.immutable

/**
  * Created by Agile Lab s.r.l. on 10/08/2017.
  */
object JsonResultsHelper extends JsonSupport {
  val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)
  val logger = WaspLogger(JsonResultsHelper.getClass.getName)

  implicit class AngularOkResponse(js: JsValue){
    def toAngularOkResponse: HttpResponse  = {
      val jsonResult = JsObject(
        "Result" -> JsString("OK"),
        "data" -> js
      )
      httpResponseJson(entity = jsonResult.toString())
    }
  }

  def angularErrorBuilder(message: String) = {
    JsObject(
      "Result" -> JsString("KO"),
      "ErrorMsg" -> JsString(message)
    )
  }

  def getJsonOrNotFound[T](result: Option[T], id: String, resource: String, converter: (T) => JsValue): HttpResponse = {
    if (result.isDefined) {
      converter(result.get).toAngularOkResponse
    } else {
      logger.info(s"$resource $id not found")
      httpResponseJson(
        entity = JsonResultsHelper.angularErrorBuilder(s"$resource $id not found").toString(),
        status = StatusCodes.NotFound
      )
    }
  }
  def getJsonArrayOrEmpty[T](result: Seq[T], converter: (Seq[T]) => JsValue): HttpResponse = {
    if (result.isEmpty) {
      JsArray().toAngularOkResponse
    } else {
      converter(result).toAngularOkResponse
    }
  }
  def runIfExists(result: Option[_], func: () => Unit, id: String, resource: String, action: String): HttpResponse = {
    if (result.isDefined) {
      func()
      "OK".toJson.toAngularOkResponse
    } else {
      logger.info(s"$resource $id not found isn't possible $action")
      httpResponseJson(
        entity = JsonResultsHelper.angularErrorBuilder(s"$resource $id not found isn't possible $action").toJson.toString(),
        status = StatusCodes.NotFound
      )
    }
  }

  def httpResponseJson(status:   StatusCode                = StatusCodes.OK,
                       headers:  immutable.Seq[HttpHeader] = Nil,
                       entity:   ResponseEntity            = HttpEntity.Empty,
                       protocol: HttpProtocol              = HttpProtocols.`HTTP/1.1`) = {
    val entityWithJson = entity.withContentType(ContentTypes.`application/json`)
    HttpResponse(status = status, headers = headers, entity = entityWithJson, protocol = protocol)
  }
}
