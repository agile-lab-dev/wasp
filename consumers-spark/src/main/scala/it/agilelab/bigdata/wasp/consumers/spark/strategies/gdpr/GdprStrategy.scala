package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr

import java.util.UUID

import it.agilelab.bigdata.wasp.consumers.spark.strategies._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.{DeletionConfig, HBaseDeletionConfig, HdfsDeletionConfig}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hbase.HBaseDeletionHandler
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.HdfsDataDeletion
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.ConfigUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hbase.HBaseUtils._
import GdprStrategy._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.HBaseConfigModel
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspDB}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
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
    val runId = ConfigUtils.getOptionalString(configuration, RUN_ID_CONFIG_KEY).getOrElse(UUID.randomUUID().toString)

    val deletionConfigs: List[DeletionConfig] = dataStores.map {
      case keyValueConf: KeyValueDataStoreConf =>
        HBaseDeletionConfig.create(configuration, keyValueConf, getKeysRDD(dataStoresDF, keyValueConf.inputKeyColumn, sparkSession), hbaseConfig)
      case rawConf: RawDataStoreConf =>
        HdfsDeletionConfig.create(configuration, rawConf, getKeys(dataStoresDF, rawConf.inputKeyColumn, sparkSession))
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

    implicit val encoder: Encoder[DeletionOutputDataFrame] = Encoders.product[DeletionOutputDataFrame]
    deletionResults.map(_.toOutputDF)
      .toDF()
      .withColumn(RUN_ID_COLUMN_NAME, lit(runId))
      .repartition(outputPartitions)
  }


  private def hdfsFailureOutput(hdfsConfig: HdfsDeletionConfig, exception: Throwable): Seq[DeletionOutput] = {
    val keyMatchType = hdfsConfig.rawMatchingStrategy match {
      case ExactRawMatchingStrategy(dataframeKeyColName) => HdfsExactColumnMatch(dataframeKeyColName)
      case PrefixRawMatchingStrategy(dataframeKeyColName) => HdfsPrefixColumnMatch(dataframeKeyColName, None)
    }
    hdfsConfig.keysToDelete.map { k =>
      DeletionOutput(k, keyMatchType, HdfsParquetSource(Seq(hdfsConfig.rawModel.uri)), DeletionFailure(exception))
    }
  }

  private def hBaseFailureOutput(hbaseConfig: HBaseDeletionConfig, exception: Throwable): Seq[DeletionOutput] = {
    val keyMatchType = hbaseConfig.keyValueMatchingStrategy match {
      case _: ExactKeyValueMatchingStrategy => HBaseExactRowKeyMatch
      case _: PrefixKeyValueMatchingStrategy => HBasePrefixRowKeyMatch(None)
      case _: PrefixAndTimeBoundKeyValueMatchingStrategy => HBasePrefixWithTimeRowKeyMatch(None)
    }

    hbaseConfig.keysWithScan.collect().map { case (key, _) =>
      DeletionOutput(key.asString, keyMatchType, HBaseTableSource(hbaseConfig.tableName), DeletionFailure(exception))
    }
  }

  /* Collect keys from `inputDF`, reading them from the column `inputKeyColumn` */
  private def getKeys(inputDF: DataFrame, inputKeyColumn: String, spark: SparkSession)
                     (implicit ev: Encoder[String]): Seq[String] = {
    inputDF
      .select(col(inputKeyColumn))
      .as[String]
      .collect()
      .toSeq
  }

  /* Retrieve keys as RDD from `inputDF`, reading them from the column `inputKeyColumn` */
  private def getKeysRDD(inputDF: DataFrame, inputKeyColumn: String, spark: SparkSession)
                        (implicit ev: Encoder[String]): RDD[String] = {
    inputDF
      .select(col(inputKeyColumn))
      .as[String]
      .rdd
  }

}

object GdprStrategy {
  val OUTPUT_PARTITIONS_CONFIG_KEY = "outputPartitions"
  val RUN_ID_CONFIG_KEY = "runId"
  val RUN_ID_COLUMN_NAME = "runId"
}