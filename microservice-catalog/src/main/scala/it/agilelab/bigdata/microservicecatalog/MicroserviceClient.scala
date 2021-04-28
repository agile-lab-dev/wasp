package it.agilelab.bigdata.microservicecatalog

import com.squareup.okhttp.{MediaType, OkHttpClient, Request, RequestBody}
import net.liftweb.json.{DefaultFormats, Serialization, parse}


/**
  * Defines functions needed by all microservices
  */
trait MicroserviceClient {
  implicit protected val formats = DefaultFormats

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
  protected def call[A, B](
                            url: String,
                            body: Option[A],
                            headers: Map[String, String],
                            method: String)
                          (implicit manifest: Manifest[B]): B = {

    //TODO: Implement header setting
    val requestBuilder = new Request.Builder().url(url)
    headers.keys.foreach(key => requestBuilder.addHeader(key, headers.get(key).get))
    val requestBody = RequestBody.create(MediaType.parse("application/json"), Serialization.write(body.getOrElse("")))
    val request = requestBuilder
      .method(method, requestBody)
      .build()
    val responseBody = new OkHttpClient().newCall(request).execute().body()
    val parsed = parse(responseBody.string())
    parsed.extract[B]
  }
}
