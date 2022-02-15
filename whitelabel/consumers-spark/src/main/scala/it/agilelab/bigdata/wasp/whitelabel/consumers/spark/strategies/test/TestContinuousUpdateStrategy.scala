package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame

class TestContinuousUpdateStrategy extends Strategy{
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    println(s"Strategy configuration: $configuration")
    val df = dataFrames.head._2
    df.sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS topic_table (id STRING, number INTEGER) STORED AS PARQUET")
    df

  }
}
