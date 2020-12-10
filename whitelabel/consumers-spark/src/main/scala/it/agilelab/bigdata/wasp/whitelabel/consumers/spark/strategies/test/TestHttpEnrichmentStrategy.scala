package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.http.Enricher
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{EnrichmentStrategy, ReaderKey}
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestDocument, TestEnrichmentResponseModel}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.immutable.Map

case class EnrichedData(text: String, textDocument: TestDocument)

class TestHttpEnrichmentStrategy  extends EnrichmentStrategy {
  /**
   *
   * @param dataFrames
   * @return
   */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    val fromKafka = dataFrames.head._2
    import fromKafka.sparkSession.implicits._

    val dataset: Dataset[TestDocument] = fromKafka.as[TestDocument]
    dataset.map { testDocument =>
      val httpGetDataEnrichment: Enricher = enricher("getHttpExample")
      val getResponseDF: TestEnrichmentResponseModel = {
        httpGetDataEnrichment.call[String, TestEnrichmentResponseModel](
          "",
          Map.apply(
            "author" -> "test",
            "version" -> "1",
            "local" -> "prova"
          ),
          Map.apply(
            "Content-type" -> "text/plain",
            "charset" -> "ISO-8859-1"
          )
        )
      }
      EnrichedData(getResponseDF.text, testDocument)
    }.toDF()
  }
}