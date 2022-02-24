package it.agilelab.bigdata.wasp.consumers.spark.http.request

import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Map

class JacksonToHttpRequestTest extends FlatSpec with Matchers {

  private val sourceKey = "httpExample"

  val config: RestEnrichmentConfigModel =
    RestEnrichmentConfigModel(
      Map.apply("httpExample" ->
        RestEnrichmentSource("http",
          Map.apply(
            "method" -> "get",
            "url" -> s"http://localhost:8080/$${author}-v1/$${version}/v2/$${local}/123?id=test_id"
          ),
          Map.apply(
            "Accept-Language" -> "en-US",
            "Authorization" -> "Basic ABC"
          )
        ),
        "msExample" ->
          RestEnrichmentSource("it.agilelab.bigdata.wasp.consumers.spark.http.CustomEnricher",
            Map.apply(
              "msName" -> "SampleMs"
            )
          )
      )
    )

  it should "test JacksonToHttpRequest" in {
    val jacksonToHttpRequest = new JacksonToHttpRequest
    val sources: RestEnrichmentSource = config.sources(sourceKey)
    val params: Map[String, String] =
      Map.apply(
        "author" -> "test",
        "version" -> "1",
        "local" -> "prova"
      )

    val headers: Map[String, String] =
      Map.apply(
        "Accept-Language" -> "it-IT",
        "Authorization" -> "Basic ABC",
        "Content-Type" -> "application/json"
      )
    val body = ""

    val request: HttpEntityEnclosingRequestBase =
      jacksonToHttpRequest.toRequest[String](sources, body, params, headers)

    request.getMethod shouldBe config.sources("httpExample").parameters("method")
    request.getURI.toString shouldBe  "http://localhost:8080/test-v1/1/v2/prova/123?id=test_id"
    request.getAllHeaders.foreach{ header =>
      (header.getName, header.getValue) shouldBe (header.getName -> headers(header.getName))
    }
  }
}
