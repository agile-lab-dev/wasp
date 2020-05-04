package it.agilelab.bigdata.wasp.consumers.spark.strategies

import java.time.Instant

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{struct, to_json}

class EventIndexingStrategy extends Strategy{

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    val withIsoTimestamp = df.drop("kafkaMetadata")
      .withColumn("timestamp_iso", EventIndexingStrategy.toIsoInstant(df.col("timestamp")))
      .drop("timestamp")
      .withColumnRenamed("timestamp_iso", "timestamp")

    withIsoTimestamp.withColumn("all", to_json(struct(withIsoTimestamp.col("*"))))

  }

}

object EventIndexingStrategy {
  import org.apache.spark.sql.functions.udf
  val toIsoInstant: UserDefinedFunction = udf[String, Long](data => Instant.ofEpochMilli(data).toString)
}