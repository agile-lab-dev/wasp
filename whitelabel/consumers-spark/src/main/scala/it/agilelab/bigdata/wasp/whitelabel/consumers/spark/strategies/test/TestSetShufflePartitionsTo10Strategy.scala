package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame

class TestSetShufflePartitionsTo10Strategy extends Strategy {
  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    df.sparkSession.sqlContext.sql(s"SET spark.sql.shuffle.partitions=10")
    df
  }
}



class TestSetShufflePartitionsTo20Strategy extends Strategy {
  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    val df = dataFrames.head._2
    df.sparkSession.sqlContext.sql(s"SET spark.sql.shuffle.partitions=20")
    df
  }
}