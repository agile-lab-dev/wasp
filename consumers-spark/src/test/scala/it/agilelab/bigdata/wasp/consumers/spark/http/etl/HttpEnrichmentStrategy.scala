package it.agilelab.bigdata.wasp.consumers.spark.http.etl

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{EnrichmentStrategy, ReaderKey}
import it.agilelab.bigdata.wasp.consumers.spark.http.Enricher
import it.agilelab.bigdata.wasp.consumers.spark.http.data.{FromKafka, SampleData}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.immutable.Map

class HttpEnrichmentStrategy extends EnrichmentStrategy {
  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    val fromKafka = dataFrames.head._2
    import fromKafka.sparkSession.implicits._

    val dataset: Dataset[FromKafka] = fromKafka.as[FromKafka]
    dataset.map{ fromKafka =>
      val httpGetDataEnrichment: Enricher = enricher("getHttpExample")
      val getResponseDF: SampleData =
        httpGetDataEnrichment.call[String, SampleData](
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

      val httpPostDataEnrichment: Enricher = enricher("postHttpExample")
      httpPostDataEnrichment.call[String, Unit](
        "test_body_post",
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

      getResponseDF
    }.toDF().join(dataset, "id")
  }
}