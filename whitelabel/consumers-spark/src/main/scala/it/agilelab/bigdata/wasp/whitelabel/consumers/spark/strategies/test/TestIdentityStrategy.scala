package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame

class TestIdentityStrategy extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    println(s"Strategy configuration: $configuration")

    dataFrames.head._2
  }
}