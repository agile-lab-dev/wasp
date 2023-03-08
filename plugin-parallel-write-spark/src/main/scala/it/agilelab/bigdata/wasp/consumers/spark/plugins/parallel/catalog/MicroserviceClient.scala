package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog

import com.squareup.okhttp.{MediaType, OkHttpClient, Request, RequestBody, Response}
import okio.BufferedSink
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URL

/**
  * Defines functions needed by all microservices
  */
trait MicroserviceClient {
  /**
    * Each microservices has a base url. This url serves as a prefix to every endpoint
    * @return microservice base url
    */
  val baseUrl: URL

  /**
    *
    * @param url Url
    * @param body Request body
    * @param headers Header map
    * @tparam A Request body type
    * @tparam B Response body type
    * @return Response body casted to B
    */
  protected def post[A: JsonFormat, B: JsonFormat](url: URL, body: Option[A], headers: Map[String, String]): B = {
    val bodyJson = body match {
      case Some(content) => content.toJson
      case None          => "".toJson
    }
    val requestBody = RequestBody.create(MediaType.parse("application/json"), bodyJson.compactPrint)

    val request = new Request.Builder()
      .url(url)
      .method("POST", requestBody)

    call[B](request, headers)
  }

  protected def postComplete[A: JsonFormat](url: URL, body: Option[A], headers: Map[String, String]): Unit = {
    val bodyJson = body match {
      case Some(content) => content.toJson
      case None          => "".toJson
    }
    val requestBody = RequestBody.create(MediaType.parse("application/json"), bodyJson.compactPrint)

    val request = new Request.Builder()
      .url(url)
      .method("POST", requestBody)

    callAndForget(request, headers)
  }

  protected def postStream(url: URL, body: BufferedSink => Unit, headers: Map[String, String]): Unit = {
    val requestBody = new RequestBody() {
      override def contentType(): MediaType          = MediaType.parse("application/octet-stream")
      override def writeTo(sink: BufferedSink): Unit = body(sink)
    }
    callAndForget(
      new Request.Builder()
        .url(url)
        .post(requestBody),
      headers
    )
  }

  /**
    *
    * @param url Url
    * @param headers Header map
    * @tparam A Response body type
    * @return Response body casted to B
    */
  protected def get[A: JsonFormat](
      url: URL,
      headers: Map[String, String]
  ): A = {
    call[A](new Request.Builder().url(url), headers)
  }


  private def callAndForget(requestBuilder: Request.Builder, headers: Map[String, String]): Unit = {
    headers.keys.foreach(key => requestBuilder.addHeader(key, headers(key)))
    val request = requestBuilder.build()
    val response: Response = new OkHttpClient().newCall(request).execute()
    if(!response.isSuccessful) {
      throw new IllegalArgumentException(
        "request - " + request.url().toString + ", " +request.body().contentType().toString+ "\tresponse - status code: " + response.code() + ", response contentType: " + response.body().contentType() + ", status description: " + response.message()
      )
    }
  }


  private def call[A: JsonFormat](request: Request.Builder, headers: Map[String, String]): A = {
    headers.keys.foreach(key => request.addHeader(key, headers(key)))
    val responseBody = new OkHttpClient().newCall(request.build()).execute().body()
    val response = responseBody.string().parseJson
    response.convertTo[A]
  }
}
