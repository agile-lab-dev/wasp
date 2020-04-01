package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr

import it.agilelab.bigdata.wasp.consumers.spark.strategies._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.{DeletionConfig, HBaseDeletionConfig, HdfsDeletionConfig}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hbase.HBaseDeletionHandler
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.HdfsDataDeletion
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.ConfigUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils._
import GdprStrategy._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.HBaseConfigModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspDB}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.bson.BsonString

import scala.util.{Failure, Success}

class GdprStrategy(dataStores: List[DataStoreConf]) extends Strategy with Logging {
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    // for each DataStore delete appropriate files
    val dataStoresDF = dataFrames.head._2
    val sparkSession = dataStoresDF.sparkSession
    import sparkSession.implicits._

    lazy val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    lazy val fileSystem = FileSystem.get(hadoopConf)
    lazy val hbaseConfig = WaspDB.getDB.getDocumentByField[HBaseConfigModel]("name", new BsonString(ConfigManager.getHBaseConfig.name))
    lazy val hdfsDataDeletion = new HdfsDataDeletion(fileSystem)

    val outputPartitions = ConfigUtils.getOptionalInt(configuration, OUTPUT_PARTITIONS_CONFIG_KEY).getOrElse(1)
    val runId = configuration.getString(RUN_ID_CONFIG_KEY)

    implicit val keyWithCorrelationEncoder: Encoder[KeyWithCorrelation] = Encoders.product[KeyWithCorrelation]

    val dataStoresWithRunIdDF = dataStoresDF.where(col("runId").equalTo(runId))

    val deletionConfigs: List[DeletionConfig] = dataStores.map {
      case keyValueConf: KeyValueDataStoreConf =>
        HBaseDeletionConfig.create(
          configuration,
          keyValueConf,
          getKeysRDD(dataStoresWithRunIdDF, keyValueConf.inputKeyColumn, keyValueConf.correlationIdColumn, sparkSession),
          hbaseConfig
        )
      case rawConf: RawDataStoreConf =>
        HdfsDeletionConfig.create(
          configuration,
          rawConf,
          getKeys(dataStoresWithRunIdDF, rawConf.inputKeyColumn, rawConf.correlationIdColumn, sparkSession)
        )
    }

    val deletionResults: Seq[DeletionOutput] = deletionConfigs.flatMap {
      case hbaseConfig: HBaseDeletionConfig =>
        HBaseDeletionHandler.delete(hbaseConfig) match {
          case Failure(exception) =>
            logger.error(s"Error during HBase deletion of $hbaseConfig", exception)
            hBaseFailureOutput(hbaseConfig, exception)
          case Success(value) => value
        }
      case hdfsConfig: HdfsDeletionConfig =>
        hdfsDataDeletion.delete(hdfsConfig, sparkSession) match {
          case Failure(exception) =>
            logger.error(s"Error during HDFS deletion of $hdfsConfig", exception)
            hdfsFailureOutput(hdfsConfig, exception)
          case Success(value) => value
        }
    }

    implicit val outputEncoder: Encoder[DeletionOutputDataFrame] = Encoders.product[DeletionOutputDataFrame]
    deletionResults.map(_.toOutputDF)
      .toDF()
      .withColumn(RUN_ID_COLUMN_NAME, lit(runId))
      .repartition(outputPartitions)
  }


  private def hdfsFailureOutput(hdfsConfig: HdfsDeletionConfig, exception: Throwable): Seq[DeletionOutput] = {
    val keyMatchType = HdfsMatchType.fromRawMatchingStrategy(hdfsConfig.rawMatchingStrategy)
    hdfsConfig.keysToDeleteWithCorrelation.map { keyWithCorrelation =>
      DeletionOutput(keyWithCorrelation, keyMatchType, HdfsRawModelSource(hdfsConfig.rawModel.uri), DeletionFailure(exception))
    }
  }

  private def hBaseFailureOutput(hbaseConfig: HBaseDeletionConfig, exception: Throwable): Seq[DeletionOutput] = {
    val keyMatchType = hbaseConfig.keyValueMatchingStrategy match {
      case _: ExactKeyValueMatchingStrategy => HBaseExactRowKeyMatch
      case _: PrefixKeyValueMatchingStrategy => HBasePrefixRowKeyMatch(None)
      case _: PrefixAndTimeBoundKeyValueMatchingStrategy => HBasePrefixWithTimeRowKeyMatch(None)
    }

    hbaseConfig.keysWithScan.collect().map { case (keyWithCorrelation, _) =>
      DeletionOutput(keyWithCorrelation, keyMatchType, HBaseTableSource(hbaseConfig.tableName), DeletionFailure(exception))
    }
  }

  /* Collect keys from `inputDF`, reading them from the column `inputKeyColumn` */
  private def getKeys(inputDF: DataFrame, inputKeyColumn: String, correlationIdColumn: String, spark: SparkSession)
                     (implicit ev: Encoder[KeyWithCorrelation]): Seq[KeyWithCorrelation] = {
    inputDF
      .select(
        col(inputKeyColumn).alias("key"),
        col(correlationIdColumn).alias("correlationId")
      )
      .as[KeyWithCorrelation]
      .collect()
      .toSeq
  }

  /* Retrieve keys as RDD from `inputDF`, reading them from the column `inputKeyColumn` */
  private def getKeysRDD(inputDF: DataFrame, inputKeyColumn: String, correlationIdColumn: String, spark: SparkSession)
                        (implicit ev: Encoder[KeyWithCorrelation]): RDD[KeyWithCorrelation] = {
    inputDF
      .select(
        col(inputKeyColumn).alias("key"),
        col(correlationIdColumn).alias("correlationId")
      )
      .as[KeyWithCorrelation]
      .rdd
  }

}

object GdprStrategy {
  type CorrelationId = String
  val OUTPUT_PARTITIONS_CONFIG_KEY = "outputPartitions"
  val RUN_ID_CONFIG_KEY = "runId"
  val RUN_ID_COLUMN_NAME = "runId"
}

case class KeyWithCorrelation(key: String, correlationId: CorrelationId) {
  def asRowKey: RowKeyWithCorrelation = RowKeyWithCorrelation(key.asRowKey, correlationId)
}

case class RowKeyWithCorrelation(rowKey: Array[Byte], correlationId: CorrelationId) {
  def asKey: KeyWithCorrelation = KeyWithCorrelation(rowKey.asString, correlationId)
}