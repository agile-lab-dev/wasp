package it.agilelab.bigdata.wasp.master.web.utils

import akka.http.scaladsl.model._
import akka.util.ByteString
import it.agilelab.bigdata.wasp.core.logging.Logging

import scala.collection.immutable


object ContentResultsHelper extends Logging {

  def getBinaryContentOrNotFound(result: Option[Array[Byte]], id: String, resource: String): HttpResponse = {
    if (result.isDefined){
      val body = ByteString(result.get)
      val byteArrayEntity = HttpEntity.Strict(MediaTypes.`application/octet-stream`, body)
      httpResponseBinaryContent(status = StatusCodes.OK, entity = byteArrayEntity)
    } else {
      logger.info(s"$resource $id not found")
      JsonResultsHelper.httpResponseJson(
        entity = JsonResultsHelper.angularErrorBuilder(s"$resource $id not found").toString(),
        status = StatusCodes.NotFound
      )
    }
  }

  def httpResponseBinaryContent(
                                 status:   StatusCode                = StatusCodes.OK,
                                 headers:  immutable.Seq[HttpHeader] = Nil,
                                 entity:   ResponseEntity            = HttpEntity.Empty,
                                 protocol: HttpProtocol              = HttpProtocols.`HTTP/1.1`
                               ): HttpResponse = {
    val entityWithApplicationType = entity.withContentType(ContentTypes.`application/octet-stream`)
    HttpResponse(status = status, headers = headers, entity = entityWithApplicationType, protocol = protocol)
  }
}
