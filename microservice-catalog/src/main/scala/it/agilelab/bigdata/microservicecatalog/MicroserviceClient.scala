package it.agilelab.bigdata.microservicecatalog

import com.squareup.okhttp.{MediaType, OkHttpClient, Request, RequestBody}
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Defines functions needed by all microservices
  */
trait MicroserviceClient {
  /**
    * Each microservices has a base url. This url serves as a prefix to every endpoint
    * @return microservice base url
    */
  protected def getBaseUrl(): String

  /**
    *
    * @param url Url
    * @param body Request body
    * @param headers Header map
    * @param method PUT/POST/GET/DELETE
    * @param manifest Response body class manifest to parse response body
    * @tparam A Request body type
    * @tparam B Response body type
    * @return Response body casted to B
    */

  protected def call[A: JsonFormat, B: JsonFormat](
                            url: String,
                            body: Option[A],
                            headers: Map[String, String],
                            method: String): B = {

    //TODO: Implement header setting
    val requestBuilder = new Request.Builder().url(url)
    headers.keys.foreach(key => requestBuilder.addHeader(key, headers.get(key).get))

    val bodyJson = body match {
      case Some(content) => content.toJson
      case None => "".toJson
    }
    val requestBody = RequestBody.create(MediaType.parse("application/json"), bodyJson.compactPrint)

    val request = requestBuilder
      .method(method, requestBody)
      .build()

    val responseBody = new OkHttpClient().newCall(request).execute().body()
    val parsed = responseBody.string().parseJson
    parsed.convertTo[B]
  }
}
