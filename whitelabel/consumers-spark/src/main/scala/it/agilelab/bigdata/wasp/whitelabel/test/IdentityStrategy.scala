package it.agilelab.bigdata.wasp.whitelabel.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.DataFrame

class IdentityStrategy extends Strategy with Logging {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = dataFrames.head._2
}
