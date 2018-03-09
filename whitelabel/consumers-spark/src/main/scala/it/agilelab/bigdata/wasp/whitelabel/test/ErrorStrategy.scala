package it.agilelab.bigdata.wasp.whitelabel.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.DataFrame

class ErrorStrategy extends Strategy with Logging {
  /**
    *
    * @param dataFrames
    * @return
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = throw new Exception("Fake error to simulate ETL component failure")
}
