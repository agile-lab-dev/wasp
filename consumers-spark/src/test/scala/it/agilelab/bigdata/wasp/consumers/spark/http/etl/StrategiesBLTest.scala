package it.agilelab.bigdata.wasp.consumers.spark.http.etl

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, equalTo, get, post, stubFor, urlEqualTo}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import it.agilelab.bigdata.wasp.consumers.spark.enrichment.enrichmentPipegraph
import it.agilelab.bigdata.wasp.consumers.spark.http.utils.{SampleEnrichmentUtil, SparkSessionTestWrapper, StrategiesUtil}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.ReaderKey
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class CustomEnrichmentStrategyTest extends FunSuite with SparkSessionTestWrapper with StrategiesUtil with SampleEnrichmentUtil {
  test("CustomEnrichmentTest") {
    val customEnrichmentStrategy = new CustomEnrichmentStrategy
    customEnrichmentStrategy.enricherConfig = enrichmentPipegraph.enrichmentSources
    val readerKey: ReaderKey = ReaderKey("topic", "fromkafka-in")
    assertResult(
      generateSingleFrameForSampleOutEv(dataOutEnv).collect()
    )(runStrategy(customEnrichmentStrategy, readerKey, dataInEv1).collect())
  }
}

class HttpEnrichmentStrategyTest extends FunSuite with BeforeAndAfterEach with SparkSessionTestWrapper with StrategiesUtil with SampleEnrichmentUtil {

  val Port = 8080
  val Host = "localhost"
  val wireMockServer = new WireMockServer(wireMockConfig().port(Port))

  override def beforeEach {
    wireMockServer.start()
    WireMock.configureFor(Host, Port)
  }

  override def afterEach {
    wireMockServer.stop()
  }
  test("HttpEnrichmentTest") {
    val path = "/test-v1/1/v2/prova/123?id=test_id"
    stubFor(get(urlEqualTo(path)).withQueryParam("id", equalTo("test_id"))
      .willReturn(
        aResponse().withBody {
          s"""
             |{
             |  "id": "abc123",
             |  "text": "Text1"
             |}
            """.stripMargin
        }))

    wireMockServer.stubFor {
      post(urlEqualTo("/test-v1/1/v2/prova/123?id=test_id"))
        .withQueryParam("id", equalTo("test_id"))
        .withHeader("charset", equalTo("ISO-8859-1"))
        .withHeader("Content-Type", equalTo("text/plain"))
        .willReturn(aResponse().withStatus(200))
    }

    val httpEnrichmentStrategy = new HttpEnrichmentStrategy
    httpEnrichmentStrategy.enricherConfig = enrichmentPipegraph.enrichmentSources
    val readerKey: ReaderKey = ReaderKey("topic", "fromkafka-in")
    assertResult(
      generateSingleFrameForSampleOutEv(dataOutEnv).collect()
    )(runStrategy(httpEnrichmentStrategy, readerKey, dataInEv1).collect())
  }
}