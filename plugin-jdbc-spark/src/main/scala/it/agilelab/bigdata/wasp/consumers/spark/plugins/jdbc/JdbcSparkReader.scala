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
    logger.info(s"Initialize Spark JDBCReader with " +
      s"\n\tconfig: $jdbcConf" +
      s"\n\tmodel: $sqlModel"
      )

    val readerOptions = getReaderOptions

    logger.info(s"Initialize Spark JDBCReader with options: $readerOptions")

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
      "url" -> getJdbcUrl(jdbcConf.dbType),
      "dbtable" -> sqlModel.dbtable,
      "user" -> jdbcConf.user,
      "password" -> jdbcConf.password,
      "driver" -> jdbcConf.driverName
    )
    val optPartitioningInfo = jdbcConf.partitioningInfo.map(
      pi =>
        Map(
          "partitionColumn" -> pi.partitionColumn,
          "lowerBound" -> pi.lowerBound,
          "upperBound" -> pi.upperBound
        )
    ).getOrElse(Map.empty[String, String])
    val optNumPartitions = jdbcConf.numPartitions.map(
      np =>
        "numPartitions" -> np.toString
    ).toMap
    val optFetchSize = jdbcConf.fetchSize.map(
      fs =>
        "fetchsize" -> fs.toString
    ).toMap

    mandatoryOpts ++ optPartitioningInfo ++ optNumPartitions ++ optFetchSize
  }
}
