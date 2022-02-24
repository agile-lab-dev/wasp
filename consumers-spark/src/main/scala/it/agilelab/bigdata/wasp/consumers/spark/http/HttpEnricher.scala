package it.agilelab.bigdata.wasp.consumers.spark.http

import it.agilelab.bigdata.wasp.consumers.spark.http.request.{JacksonToHttpRequest, ToHttpRequest}
import it.agilelab.bigdata.wasp.consumers.spark.http.response.{FromHttpResponse, JacksonFromHttpResponse}
import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentSource
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{CloseableHttpResponse, HttpEntityEnclosingRequestBase}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}

import scala.reflect.ClassTag

class HttpEnricher(sourceInfo: RestEnrichmentSource) extends Enricher {

  val httpClient: CloseableHttpClient = HttpClients.createDefault()

  def fromInputToRequest[A: ClassTag](
                                       body: A,
                                       params: Map[String, String],
                                       headers: Map[String, String]
                                     ): HttpEntityEnclosingRequestBase = {
    val toHttpRequest: ToHttpRequest =
      sourceInfo.parameters.get("toRequestClass") match {
        case Some(className) => Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[ToHttpRequest]
        case None => new JacksonToHttpRequest
      }

    toHttpRequest.toRequest[A](
      sourceInfo,
      body,
      params,
      headers
    )
  }

  private def fromResponseToOutput[B: ClassTag](response: HttpResponse): B = {
    val toResponse: FromHttpResponse =
      sourceInfo.parameters.get("fromResponseClass") match {
        case None => new JacksonFromHttpResponse
        case Some(className) => Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[FromHttpResponse]
      }

    toResponse.fromResponse[B](response)
  }

  override def call[A: ClassTag, B: ClassTag](
                                               body: A,
                                               params: Map[String, String],
                                               headers: Map[String, String]
                                             ): B = {
    val request: HttpEntityEnclosingRequestBase = fromInputToRequest(body, params, headers)
    val response: CloseableHttpResponse = httpClient.execute(request)
    try {
      fromResponseToOutput[B](response)
    } finally {
      response.close()
    }
  }

  override def close(): Unit = {
    httpClient.close()
  }
}
