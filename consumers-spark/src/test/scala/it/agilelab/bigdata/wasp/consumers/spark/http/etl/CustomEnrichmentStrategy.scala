package it.agilelab.bigdata.wasp.consumers.spark.http.etl

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{EnrichmentStrategy, ReaderKey}
import it.agilelab.bigdata.wasp.consumers.spark.http.Enricher
import it.agilelab.bigdata.wasp.consumers.spark.http.data.{FromKafka, SampleData}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.immutable.Map

class CustomEnrichmentStrategy extends EnrichmentStrategy {
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
      val exampleDataEnrichment: Enricher = enricher("msExample")
      exampleDataEnrichment.call[String, SampleData](
        "",
        Map.apply("author" -> fromKafka.exampleAuthor),
        Map.empty)
    }.toDF().join(dataset, "id")
  }

}