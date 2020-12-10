package it.agilelab.bigdata.wasp.consumers.spark.http.utils

import it.agilelab.bigdata.wasp.consumers.spark.http.data.{EnrichedData, FromKafka}
import org.apache.spark.sql.DataFrame

trait SampleEnrichmentUtil { _: SparkSessionTestWrapper =>

  protected val dataInEv1 = {
    FromKafka("abc123", "Author1", 1L)
  }
  protected val dataInEv2 = {
    FromKafka("def456", "Author2", 2L)
  }

  protected val dataOutEnv =  {
    EnrichedData("abc123", "Text1", "Author1", 1L)
  }

  protected def generateSingleFrameForSampleOutEv(data: EnrichedData): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(data)))

}
