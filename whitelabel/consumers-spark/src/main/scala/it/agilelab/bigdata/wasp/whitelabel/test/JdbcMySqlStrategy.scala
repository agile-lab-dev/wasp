package it.agilelab.bigdata.wasp.whitelabel.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.whitelabel.models.test.TestSqlSouceModel
import org.apache.spark.sql.DataFrame

class JdbcMySqlStrategy extends Strategy with Logging {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    // Retrieve 'database' config 'jdbc.connections.<connectionName>.url' (e.g. "jdbc:mysql://mysql:<port>/<db>")
    val connectionUrl = ConfigManager.getJdbcConfig.connections(TestSqlSouceModel.mySql.connectionName).url
    val database = connectionUrl.substring(connectionUrl.lastIndexOf("/")+1 , connectionUrl.length)
    logger.info(s"Retrieved 'database': ${database}")

    // do some stuff

    dataFrames.head._2
  }
}