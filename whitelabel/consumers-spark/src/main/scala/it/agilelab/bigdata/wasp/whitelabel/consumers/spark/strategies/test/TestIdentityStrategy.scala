package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.DataFrame

class TestIdentityStrategy extends Strategy with Logging {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    logger.info(s"Strategy configuration: ${configuration}")

    dataFrames.head._2
  }
}
