package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.functions.{array, lit, struct}
import org.apache.spark.sql.{DataFrame, Encoders}

class TestToKafkaPlainStrategy extends Strategy {
  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val ss = dataFrames.head._2.sparkSession
    ss.createDataset(List.fill(100)("A"-> "B"))(Encoders.product)
      .withColumnRenamed("_1", "myKey")
      .withColumn("myHeaders", array(struct(lit("key").as("headerKey"), lit("value".getBytes).as("headerValue")),
        struct(lit("key2").as("headerKey"), lit("value2".getBytes).as("headerValue"))))
      .withColumnRenamed("_2", "myValue")
  }
}
