package it.agilelab.bigdata.wasp.consumers.spark.strategies

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, struct, to_json}

class TelemetryIndexingStrategy extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    val withMetricsSearchKey = df
      .drop("kafkaMetadata")
      .withColumn("metricSearchKey", concat(col("sourceId"), lit("|"), col("metric")))

    withMetricsSearchKey.withColumn("all", to_json(struct(withMetricsSearchKey.col("*"))))

  }

}
