package it.agilelab.bigdata.wasp.consumers.spark.http.request

import java.net.{URI, URL}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import it.agilelab.bigdata.wasp.consumers.spark.http.utils.HttpEnricherUtils
import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentSource
import org.apache.http.HttpEntity
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase

import scala.reflect.ClassTag
import scala.util.{Failure, Try}

class JacksonToHttpRequest extends ToHttpRequest {

  private val HttpMethods = Seq("get", "post", "put", "patch", "delete")

  private def extractHttpMethod(conf: RestEnrichmentSource): Try[String] = Try {
    conf.parameters.get("method") match {
      case Some(method) => method match {
        case str if !str.isEmpty && HttpMethods.contains(str.toLowerCase()) => str
        case _ => throw new Exception(s"Cannot retrieve http method due to empty string value")
      }
      case None => throw new Exception(s"Http method field not found")
    }
  }

  private def getHttpMethod(conf: RestEnrichmentSource): Try[String] = {
    for {
      httpMethod <- extractHttpMethod(conf).recoverWith {
        case e: Throwable =>
          Failure(new Exception(s"Failed to extract http method from configurations", e))
      }
    } yield httpMethod
  }

  private def serialize[A](body: A): String = {
    val mapper = new ObjectMapper
    mapper
      .registerModule(DefaultScalaModule)
      .writeValueAsString(body)
  }

  override def toRequest[A: ClassTag](
                                       conf: RestEnrichmentSource,
                                       body: A,
                                       params: Map[String, String],
                                       headers: Map[String, String]
                                     ): HttpEntityEnclosingRequestBase = {

    val httpEntityEnclosingRequestBase = new HttpEntityEnclosingRequestBase {
      override def getMethod: String = getHttpMethod(conf).get
    }

    val requestEntity: HttpEntity =
      EntityBuilder
        .create()
        .setText(serialize[A](body))
        .build()
    httpEntityEnclosingRequestBase.setEntity(requestEntity)

    conf.parameters.get("url") match {
      case Some(url) =>
        val urlReq = new URL(url)
        httpEntityEnclosingRequestBase.setURI(
          new URI(
            urlReq.getProtocol,
            urlReq.getUserInfo,
            urlReq.getHost,
            urlReq.getPort,
            HttpEnricherUtils.resolveUrlPath(urlReq.getPath, params),
            urlReq.getQuery,
            urlReq.getRef)
        )
      case None => throw new Exception("url not found")
    }

    HttpEnricherUtils.mergeHeaders(headers, conf.headers)
      .foreach {
        case(name, value) => httpEntityEnclosingRequestBase.setHeader(name, value)
      }

    httpEntityEnclosingRequestBase
  }
}
