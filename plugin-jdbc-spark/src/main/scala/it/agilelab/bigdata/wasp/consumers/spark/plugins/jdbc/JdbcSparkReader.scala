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

  override def read(sc: SparkContext): DataFrame = {
    logger.info(s"Initialize Spark JDBCReader with " +
      s"\n\tconfig: $jdbcConfig" +
      s"\n\tmodel: $sqlModel"
      )

    val readerOptions = getReaderOptions

    //Workaround SparkSession retrieval
    val ss: SparkSession = new SQLContext(sc).sparkSession

    val jdbcDf: DataFrame= ss.read
      .format("jdbc")
      .options(readerOptions)
      .load

    jdbcDf
  }

  private def getReaderOptions = {
    val mandatoryOpts = Map(
      "url" -> s"jdbc:${jdbcConfig.dbType}:${jdbcConfig.connectionString}/${sqlModel.database}",
      "dbtable" -> sqlModel.dbtable,
      "user" -> jdbcConfig.user,
      "password" -> jdbcConfig.password,
      "driverName" -> jdbcConfig.driverName
    )
    val optPartitioningInfo = jdbcConfig.partitioningInfo.map(
      pi =>
        Map(
          "partitionColumn" -> pi.partitionColumn,
          "lowerBound" -> pi.lowerBound,
          "upperBound" -> pi.upperBound
        )
    ).getOrElse(Map.empty[String, String])
    val optNumPartitions = jdbcConfig.numPartitions.map(
      np =>
        "numPartitions" -> np.toString
    ).toMap
    val optFetchSize = jdbcConfig.fetchSize.map(
      fs =>
        "fetchsize" -> fs.toString
    ).toMap

    mandatoryOpts ++ optPartitioningInfo ++ optNumPartitions ++ optFetchSize
  }
}
