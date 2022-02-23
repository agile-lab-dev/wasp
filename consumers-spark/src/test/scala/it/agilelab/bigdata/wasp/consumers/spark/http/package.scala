package it.agilelab.bigdata.wasp.consumers.spark

import it.agilelab.bigdata.wasp.consumers.spark.http.etl.sampleStreamingETL
import it.agilelab.bigdata.wasp.models.PipegraphModel
import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}

import scala.collection.immutable.Map

package object enrichment {

  val enrichmentPipegraph: PipegraphModel = PipegraphModel(
    name = "sbs-data-pipegraph",
    description = "Sample enrichment pipegraph",
    owner = "",
    isSystem = false,
    creationTime = System.currentTimeMillis(),
    structuredStreamingComponents = List(sampleStreamingETL),
    enrichmentSources =
      RestEnrichmentConfigModel(
        Map.apply(
          "getHttpExample" ->
            RestEnrichmentSource("http",
              Map.apply(
                "method" -> "get",
                "url" -> "http://localhost:8080/${author}-v1/${version}/v2/${local}/123?id=test_id"
              ),
              Map.apply(
                "Content-type" -> "text/plain",
                "charset" -> "ISO-8859-1"
              )
            ),
          "postHttpExample" ->
            RestEnrichmentSource("http",
              Map.apply(
                "method" -> "post",
                "url" -> "http://localhost:8080/${author}-v1/${version}/v2/${local}/123?id=test_id"
              ),
              Map.apply(
                "Content-type" -> "text/plain",
                "charset" -> "ISO-8859-1"
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
  )
}
