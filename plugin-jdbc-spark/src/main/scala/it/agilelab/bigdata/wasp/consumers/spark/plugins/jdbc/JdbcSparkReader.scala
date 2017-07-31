package it.agilelab.bigdata.wasp.consumers.spark.plugins.jdbc

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.SqlSourceModel
import it.agilelab.bigdata.wasp.core.utils.JdbcConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

class JdbcSparkReader(sqlModel: SqlSourceModel) extends SparkReader with JdbcConfiguration with Logging {
  override val name: String = sqlModel.name
  override val readerType: String = "jdbc"
  val jdbcConf = jdbcConfigList.connections(sqlModel.connectionName)

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

    val jdbcDf: DataFrame= ss.read
      .format("jdbc")
      .options(readerOptions)
      .load

    jdbcDf
  }

  private def getReaderOptions = {

    def getJdbcUrl(dbType: String): String = dbType match {
        // Different db have different connection url structure. List to be updated
      case "oracle" => s"jdbc:oracle:thin://@${jdbcConf.connectionString}/${sqlModel.database}"
      case _ => throw new UnsupportedOperationException(s"jdbc db type '$dbType' not supported in jdbc reader")
    }

    //jdbc:oracle:thin://@<hostname>:1521/<db>
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
