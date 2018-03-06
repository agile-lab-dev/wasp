package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
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
class JdbcSparkReader(sqlModel: SqlSourceModel) extends SparkReader with JdbcConfiguration with Logging {
  val name: String = sqlModel.name
  val readerType: String = Datastores.jdbcProduct

  if(! jdbcConfig.connections.isDefinedAt(sqlModel.connectionName)) {
    val msg = s"Jdbc spark reader connectionName not found: '${sqlModel.connectionName}'"
    logger.error(msg)
    throw new Exception(msg)
  }
  val connection = jdbcConfig.connections(sqlModel.connectionName)

  override def read(sc: SparkContext): DataFrame = {
    logger.info(s"Initialize Spark JDBCReader with " +
                s"\n\tconfig: $connection" +
                s"\n\tmodel: $sqlModel")

    val readerOptions = getReaderOptions

    logger.info(s"Initialize Spark JDBCReader with options: $readerOptions")

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
      "url" -> connection.url, //e.g. "jdbc:oracle:thin://@<hostname>:<port>/<db>"
      "dbtable" -> sqlModel.dbtable,  // retrieved from SqlSourceModel
      "user" -> connection.user,
      "password" -> connection.password,
      "driver" -> connection.driverName //e.g. "oracle.jdbc.driver.OracleDriver"
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