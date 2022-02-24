package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.whitelabel.models.test.TestDocumentWithMetadata
import org.apache.spark.sql.DataFrame

class TestEchoStrategy extends Strategy {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    println(s"Strategy configuration: $configuration")

    val dataFrame = dataFrames.head._2
    import dataFrame.sparkSession.implicits._

    dataFrame.as[TestDocumentWithMetadata].map(t=> {
      println(t)
      t
    }).toDF()
  }
}