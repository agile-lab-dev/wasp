package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct.JDBCProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{Datastores, SqlSourceModel}
import it.agilelab.bigdata.wasp.core.utils.JdbcConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * It read data from JDBC with the configuration of JDBCConfiguration.
  * It use the push down method of SparkSQL to convert SQL to JDBC query
  *
  * @param sqlModel JDBC configuration
  */
class JDBCSparkBatchReader(sqlModel: SqlSourceModel) extends SparkBatchReader with JdbcConfiguration with Logging {
  val name: String = sqlModel.name
  val readerType: String = JDBCProduct.getActualProduct

  if(! jdbcConfig.connections.isDefinedAt(sqlModel.connectionName)) {
    val msg = s"Jdbc spark reader connectionName not found: '${sqlModel.connectionName}'"
    logger.error(msg)
    throw new Exception(msg)
  }
  val connection = jdbcConfig.connections(sqlModel.connectionName)

  override def read(sc: SparkContext): DataFrame = {
    val connectionObfuscated = connection.copy(user = "***", password = "***")
    logger.info(s"Initialize Spark JDBCReader with " +
                s"\n\tconfig (obfuscated): $connectionObfuscated" +
                s"\n\tmodel: $sqlModel")

    val readerOptions: Map[String, String] = getReaderOptions
    val readerOptionsObfuscated = readerOptions.map(
      ro => if((ro._1 =="user") || (ro._1 =="password")) ro._1 -> "***" else ro
    )
    logger.info(s"Initialize Spark JDBCReader with options (obfuscated): $readerOptionsObfuscated")

    //Workaround SparkSession retrieval
    val ss: SparkSession = new SQLContext(sc).sparkSession

    val df: DataFrame =
      ss.read
        .format("jdbc")
        .options(readerOptions)
        .load

    df
  }

  private def getReaderOptions = {

    /* All retrieved from config except "dbtable" */
    val mandatoryOpts = Map(
      "url" -> connection.url,            // e.g. "jdbc:mysql://mysql:<port>/<db>", "jdbc:oracle:thin://@<hostname>:<port>/<db>"
      "user" -> connection.user,
      "password" -> connection.password,
      "driver" -> connection.driverName,  // e.g. "com.mysql.jdbc.Driver", "oracle.jdbc.driver.OracleDriver"
      "dbtable" -> sqlModel.dbtable       // retrieved from SqlSourceModel
    )

    val optPartitioningInfo = sqlModel.partitioningInfo.map(
      pi =>
        Map(
          "partitionColumn" -> pi.partitionColumn,
          "lowerBound" -> pi.lowerBound,
          "upperBound" -> pi.upperBound
        )
    ).getOrElse(Map.empty[String, String])

    val optNumPartitions = sqlModel.numPartitions.map(
      np =>
        "numPartitions" -> np.toString
    ).toMap

    val optFetchSize = sqlModel.fetchSize.map(
      fs =>
        "fetchsize" -> fs.toString
    ).toMap

    mandatoryOpts ++ optPartitioningInfo ++ optNumPartitions ++ optFetchSize
  }
}