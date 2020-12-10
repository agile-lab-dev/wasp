package it.agilelab.bigdata.wasp.consumers.spark.http

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, _}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import it.agilelab.bigdata.wasp.consumers.spark.http.data.SampleData
import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.immutable.Map

class HttpEnricherTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  val Port = 8080
  val Host = "localhost"
  val wireMockServer = new WireMockServer(wireMockConfig().port(Port))

  private val sourceKey = "httpExample"

  val config: RestEnrichmentConfigModel =
    RestEnrichmentConfigModel(
      Map.apply("httpExample" ->
        RestEnrichmentSource("http",
          Map.apply(
            "method" -> "get",
            "url" -> "http://localhost:8080/${author}-v1/${version}/v2/${local}/123?id=test_id"
          ),
          Map.apply(
            "Accept-Language" -> "en-US"
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
      "Content-Type" -> "application/json"
    )
  val body = ""

  val httpEnricher = new HttpEnricher(sources)

  override def beforeEach {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
  }

  override def afterEach {
    wireMockServer.stop()
  }

  "WireMock" should "stub get request" in {
    val path = "/test-v1/1/v2/prova/123?id=test_id"
    stubFor(get(urlEqualTo(path))
      .willReturn(
        aResponse().withBody {
          s"""
             |{
             |  "id": "abc123",
             |  "text": "Author1"
             |}
          """.stripMargin
        }))

    val response = httpEnricher.call[String, SampleData](body, params, headers)
    response.id shouldBe "abc123"
    response.text shouldBe "Author1"
  }
}
