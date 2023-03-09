package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr

import it.agilelab.bigdata.wasp.consumers.spark.strategies._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.{DeletionConfig, HBaseDeletionConfig, HdfsDeletionConfig}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hbase.HBaseDeletionHandler
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.HdfsDataDeletion
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.ConfigUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils._
import GdprStrategy._
import it.agilelab.bigdata.wasp.consumers.spark.batch.AggregateException
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.{DataStoreConf, ExactKeyValueMatchingStrategy, KeyValueDataStoreConf, PrefixAndTimeBoundKeyValueMatchingStrategy, PrefixKeyValueMatchingStrategy, RawDataStoreConf}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

class GdprStrategy(dataStores: List[DataStoreConf]) extends Strategy with HasPostMaterializationHook with Logging {

  /**
    *
    * @param maybeDataframe (optional) output dataframe of the strategy (it is None only if the strategy has no writer)
    * @param maybeError     Some(error) if an error happened during materialization; None otherwise
    * @return the wanted outcome of the strategy. This means that if the maybeError is not None and the hook does not
    *         have a "recovery" logic, it should always return a Failure wrapping the input error.
    */
  override def postMaterialization(maybeDataframe: Option[DataFrame],
                                   maybeError: Option[Throwable]): Try[Unit] = {
    val errors = maybeError.map(Failure.apply).toList :::
      postWriteActions.map(_.apply()).collect { case f@Failure(_) => f }
    if (errors.isEmpty) {
      Success(())
    } else {
      Failure(AggregateException("At least one shutdown action failed", errors.map(_.exception)))
    }
  }

  private var postWriteActions: List[() => Try[Unit]] = List.empty

  private def addPostWriteAction[A](action: () => A): Unit = {
    addPostWriteTryAction(() => Try(action.apply()))
  }

  private def addPostWriteTryAction[A](action: () => Try[A]): Unit = {
    val unitAction: () => Try[Unit] = () => action.apply().map(_ => ())
    postWriteActions = unitAction :: postWriteActions
  }

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    // for each DataStore delete appropriate files
    val dataStoresDF = dataFrames.head._2
    val sparkSession = dataStoresDF.sparkSession
    import sparkSession.implicits._

    lazy val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    lazy val fileSystem = FileSystem.get(hadoopConf)
    lazy val hbaseConfig = Option.apply(ConfigManager.getHBaseConfig)
    lazy val hdfsDataDeletion = new HdfsDataDeletion(fileSystem)
    val STORAGE_LEVEL = "storageLevel"
    val storageLevel =
      ConfigUtils.getOptionalString(configuration, STORAGE_LEVEL).flatMap { s =>
        Try(StorageLevel.fromString(s)).recoverWith {
          case e =>
            logger.warn(e.getMessage, e)
            Failure(e)
        }.toOption
      }.getOrElse(StorageLevel.MEMORY_AND_DISK)

    val outputPartitions = ConfigUtils.getOptionalInt(configuration, OUTPUT_PARTITIONS_CONFIG_KEY).getOrElse(1)
    val runId = configuration.getString(RUN_ID_CONFIG_KEY)

    val dataStoresWithRunIdDF = dataStoresDF.where(col("runId").equalTo(runId))
    val seqWithRunIdDF = dataStoresWithRunIdDF.collect().toSeq

    val deletionConfigs: List[DeletionConfig] = dataStores.map {
      case keyValueConf: KeyValueDataStoreConf =>
        HBaseDeletionConfig.create(
          configuration,
          keyValueConf,
          getKeysRDD(seqWithRunIdDF, keyValueConf.inputKeyColumn, keyValueConf.correlationIdColumn, sparkSession),
          hbaseConfig
        )
      case rawConf: RawDataStoreConf =>
        HdfsDeletionConfig.create(
          configuration,
          rawConf,
          getKeys(seqWithRunIdDF, rawConf.inputKeyColumn, rawConf.correlationIdColumn)
        )
    }

    val deletionResults: RDD[DeletionOutput] = deletionConfigs.map {
      case hbaseConfig: HBaseDeletionConfig =>
        HBaseDeletionHandler.delete(hbaseConfig, storageLevel) match {
          case Failure(exception) =>
            logger.error(s"Error during HBase deletion of $hbaseConfig", exception)
            sparkSession.sparkContext.makeRDD(hBaseFailureOutput(hbaseConfig, exception))
          case Success(value) =>
            addPostWriteAction { () =>
              logger.info(s"Unpersisting RDD $value")
              value.unpersist()
            }
            value
        }
      case hdfsConfig: HdfsDeletionConfig =>
        hdfsDataDeletion.delete(hdfsConfig, sparkSession) match {
          case Failure(exception) =>
            logger.error(s"Error during HDFS deletion of $hdfsConfig", exception)
            sparkSession.sparkContext.makeRDD(hdfsFailureOutput(hdfsConfig, exception))
          case Success(value) => sparkSession.sparkContext.makeRDD(value)
        }
    }.reduce(_ union _)

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
  private def getKeys(inputRows: Seq[Row], inputKeyColumn: String, correlationIdColumn: String): Seq[KeyWithCorrelation] = {
    inputRows.map { r =>
      KeyWithCorrelation(
        r.getAs[String](inputKeyColumn),
        r.getAs[String](correlationIdColumn)
      )
    }
  }

  /* Retrieve keys as RDD from `inputDF`, reading them from the column `inputKeyColumn` */
  private def getKeysRDD(inputRows: Seq[Row], inputKeyColumn: String, correlationIdColumn: String, spark: SparkSession): RDD[KeyWithCorrelation] = {

    spark.sparkContext.makeRDD(getKeys(inputRows, inputKeyColumn, correlationIdColumn))
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