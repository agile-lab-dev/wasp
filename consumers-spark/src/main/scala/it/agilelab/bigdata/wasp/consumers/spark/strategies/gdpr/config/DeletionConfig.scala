package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Locale

import com.typesafe.config.{Config, ConfigException}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.exception.ConfigExceptions.{KeyValueConfigException, RawDataConfigException}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.ConfigUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.{KeyWithCorrelation, RowKeyWithCorrelation}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.HBaseConfigModel
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter, PrefixFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Contains
import org.apache.spark.sql.functions.{expr, lit}

import scala.util.{Failure, Success}

sealed trait DeletionConfig

/**
  * Contains the configuration settings for an HBase Deletion Job
  *
  * @param keysWithScan             list of distinct keys to delete (from config or input model), together with the HBase scan
  *                                 that will be used to match the HBase rowKeys to delete
  * @param tableName                namespace:table name of the HBase table to handle derived from the KeyValueModel
  * @param hbaseConfigModel         HBaseConfigModel that will be used to create an HBaseConnection inside the executors
  * @param keyValueMatchingStrategy KeyValueMatchingStrategy defined in the BatchJobModel
  */
case class HBaseDeletionConfig(
                                keysWithScan: RDD[(KeyWithCorrelation, Scan)],
                                tableName: String,
                                hbaseConfigModel: Option[HBaseConfigModel],
                                keyValueMatchingStrategy: KeyValueMatchingStrategy
                              ) extends DeletionConfig

/**
  * Contains the configuration settings for an Hdfs Deletion Job
  *
  * @param keysToDeleteWithCorrelation list of distinct keys to delete (from config or input model)
  * @param rawModel                    RawModel to handle
  * @param rawMatchingStrategy         RawMatchingStrategy defined in the BatchJobModel
  * @param rawMatchingCondition        WHERE condition derived from the RawMatchingStrategy
  * @param partitionPruningCondition   WHERE condition derived from the PartitionPruningStrategy
  * @param stagingDirUri               staging directory path to use (from config or default = rawModel.uri + "/staging")
  * @param backupDirUri                backup directory parent path to use (from config or default = rawModel.uri.parent + "/staging")
  * @param missingPathFailure          if true a missing path inside [[rawModel]] results in deletion failure
  */
case class HdfsDeletionConfig(
                               keysToDeleteWithCorrelation: Seq[KeyWithCorrelation],
                               rawModel: RawModel,
                               rawMatchingStrategy: RawMatchingStrategy,
                               rawMatchingCondition: Column,
                               partitionPruningCondition: Column,
                               stagingDirUri: String,
                               backupDirUri: String,
                               missingPathFailure: Boolean = false
                             ) extends DeletionConfig {
  // WHERE condition derived from the RawMatchingStrategy
  def joinCondition(dataKeyColumn: Column, inputKeyColumn: Column): Column = {
    rawMatchingStrategy match {
      case ExactRawMatchingStrategy(_) =>
        dataKeyColumn.equalTo(inputKeyColumn)
      case PrefixRawMatchingStrategy(_) =>
        dataKeyColumn.startsWith(inputKeyColumn)
      case ContainsRawMatchingStrategy(_) =>
        new Column(Contains(dataKeyColumn.expr, inputKeyColumn.expr))
      //        dataKeyColumn.contains(inputKeyColumn)
    }
  }
}

object HdfsDeletionConfig {
  val RAW_CONF_KEY = "hdfs"
  val DEFAULT_STAGING = "/staging"
  val STAGING_DIR_KEY = "stagingDir"
  val BACKUP_DIR_KEY = "backupDir"
  val KEYS_TO_DELETE_KEY = "keys"
  val CORRELATION_ID_KEY = "correlationId"
  val START_PERIOD_KEY = "start"
  val END_PERIOD_KEY = "end"
  val TIMEZONE_PERIOD_KEY = "timeZone"
  val ALWAYS_TRUE_COLUMN: Column = lit(true) === lit(true)

  def create(rootConfig: Config,
             rawDataStoreConf: RawDataStoreConf,
             inputKeys: => Seq[KeyWithCorrelation]): HdfsDeletionConfig = {
    val maybeConfig: Option[Config] = ConfigUtils.getOptionalConfig(rootConfig, RAW_CONF_KEY)
    val keysToDelete = ConfigUtils.keysToDelete(inputKeys, maybeConfig, KEYS_TO_DELETE_KEY, CORRELATION_ID_KEY).distinct

    new HdfsDeletionConfig(
      keysToDelete,
      rawDataStoreConf.rawModel,
      rawDataStoreConf.rawMatchingStrategy,
      rawMatchingCondition(keysToDelete.map(_.key), rawDataStoreConf.rawMatchingStrategy),
      partitionPruningCondition(maybeConfig, rawDataStoreConf.partitionPruningStrategy),
      stagingDirUri(maybeConfig, rawDataStoreConf.rawModel),
      backupDirUri(maybeConfig, rawDataStoreConf.rawModel),
      rawDataStoreConf.missingPathFailure
    )
  }

  private def rawMatchingCondition(keysToDelete: Seq[String], rawMatchingStrategy: RawMatchingStrategy): Column = {
    rawMatchingStrategy match {
      case ExactRawMatchingStrategy(dataframeKeyMatchingExpression) =>
        expr(dataframeKeyMatchingExpression) isin (keysToDelete: _*)
      case PrefixRawMatchingStrategy(dataframeKeyMatchingExpression) =>
        val regex = keysToDelete.mkString("^", "|^", "")
        expr(dataframeKeyMatchingExpression) rlike regex
      case ContainsRawMatchingStrategy(dataframeKeyMatchingExpression) =>
        val expression = expr(dataframeKeyMatchingExpression)
        keysToDelete.foldLeft(lit(false)) {
          case (acc, key) => acc or expression.contains(key)
        }
    }
  }

  /* Generate the partition pruning condition based on the PartitionPruningStrategy defined and the configuration */
  private def partitionPruningCondition(maybeConfig: Option[Config],
                                        partitionPruningStrategy: PartitionPruningStrategy): Column = {
    partitionPruningStrategy match {
      case p: TimeBasedBetweenPartitionPruningStrategy =>
        val config = maybeConfig match {
          case Some(x) => x
          case None => throw RawDataConfigException(new ConfigException.Missing(RAW_CONF_KEY))
        }
        val timeZone = try {
          config.getString(TIMEZONE_PERIOD_KEY)
        } catch {
          case _: ConfigException.Missing => "UTC"
        }
        p.condition(
          wrapConfigException(config.getLong(START_PERIOD_KEY)),
          wrapConfigException(config.getLong(END_PERIOD_KEY)),
          timeZone
        )
      case NoPartitionPruningStrategy() => ALWAYS_TRUE_COLUMN
    }
  }

  private def stagingDirUri(maybeConfig: Option[Config], rawModel: RawModel): String = {
    maybeConfig
      .flatMap(ConfigUtils.getOptionalString(_, STAGING_DIR_KEY))
      .getOrElse(createDefaultStagingDirUri(rawModel))
  }

  private def backupDirUri(maybeConfig: Option[Config], rawModel: RawModel): String = {
    maybeConfig
      .flatMap(ConfigUtils.getOptionalString(_, BACKUP_DIR_KEY))
      .getOrElse(createDefaultBackupDirUri(rawModel))
  }


  private def createDefaultStagingDirUri(rawModel: RawModel): String = {
    rawModel.uri.stripSuffix("/") + DEFAULT_STAGING
  }

  private def createDefaultBackupDirUri(rawModel: RawModel): String = {
    new Path(rawModel.uri).getParent.toUri.toString
  }

  private def wrapConfigException[T](value: => T): T = try {
    value
  } catch {
    case e: ConfigException => throw RawDataConfigException(e)
  }

}


object HBaseDeletionConfig extends Logging {
  val KV_CONF_KEY = "hbase"
  val KEYS_TO_DELETE_KEY = "keys"
  val CORRELATION_ID_KEY = "correlationId"
  val START_PERIOD_KEY = "start"
  val END_PERIOD_KEY = "end"
  val TIMEZONE_PERIOD_KEY = "timeZone"
  val BATCH_SIZE = "batchSize"

  def create(rootConfig: Config,
             keyValueDataStoreConf: KeyValueDataStoreConf,
             inputKeys: RDD[KeyWithCorrelation],
             hBaseConfigModel: Option[HBaseConfigModel]): HBaseDeletionConfig = {
    val maybeConfig: Option[Config] = ConfigUtils.getOptionalConfig(rootConfig, KV_CONF_KEY)
    val rowKeys = ConfigUtils.keysToDeleteRDD(inputKeys, maybeConfig, KEYS_TO_DELETE_KEY, CORRELATION_ID_KEY).distinct.map {
      case KeyWithCorrelation(key, correlationId) => RowKeyWithCorrelation(key.asRowKey, correlationId)
    }

    val keysWithScan: RDD[(RowKeyWithCorrelation, Scan)] = keyValueDataStoreConf.keyValueMatchingStrategy match {
      case _: ExactKeyValueMatchingStrategy => scanExact(rowKeys, maybeConfig)
      case _: PrefixKeyValueMatchingStrategy => scanPrefix(rowKeys, maybeConfig)
      case prefixAndTime: PrefixAndTimeBoundKeyValueMatchingStrategy =>
        val config = maybeConfig match {
          case Some(x) => x
          case None => throw KeyValueConfigException(new ConfigException.Missing(KV_CONF_KEY), "Missing mandatory configuration key")
        }
        scanPrefixWithTime(config, rowKeys, prefixAndTime)
    }

    val tableName = KeyValueModel.extractTableName(keyValueDataStoreConf.keyValueModel.tableCatalog) match {
      case Failure(exception) => throw KeyValueConfigException(exception, "Impossible to extract HBase table name from KeyValueModel")
      case Success(value) => value
    }

    new HBaseDeletionConfig(
      keysWithScan.map { case (rowKeyWithCorrelation, scan) => rowKeyWithCorrelation.asKey -> scan },
      tableName,
      hBaseConfigModel,
      keyValueDataStoreConf.keyValueMatchingStrategy
    )
  }

  /* For each key to delete inside the RDD, creates an HBase Scan that matches it exactly with a rowkey */
  private def scanExact(keysToDelete: RDD[RowKeyWithCorrelation], conf: Option[Config]): RDD[(RowKeyWithCorrelation, Scan)] = {
    keysToDelete.map { case rowKeyWithCorrelation@RowKeyWithCorrelation(rowKey, _) =>
      val scan = new Scan(rowKey, rowKey)
      conf.foreach(ConfigUtils.getOptionalInt(_, BATCH_SIZE).foreach(scan.setBatch))
      rowKeyWithCorrelation -> scan.setFilter(
        new FilterList(
          new KeyOnlyFilter(),
          new FirstKeyOnlyFilter()
        )
      )
    }
  }

  /* For each key to delete inside the RDD, creates an HBase Scan that matches any rowkey that has the same prefix of the key to delete */
  private def scanPrefix(keysToDelete: RDD[RowKeyWithCorrelation], conf: Option[Config]): RDD[(RowKeyWithCorrelation, Scan)] = {
    keysToDelete.map { case rowKeyWithCorrelation@RowKeyWithCorrelation(rowKey, _) =>
      val scan = new Scan(rowKey, rowKey ++ Array(Byte.MaxValue))
      conf.foreach(ConfigUtils.getOptionalInt(_, BATCH_SIZE).foreach(scan.setBatch))
      rowKeyWithCorrelation -> scan.setFilter(
        new FilterList(
          new PrefixFilter(rowKey)
        )
      )
    }
  }

  /* For each key to delete inside the RDD, creates an HBase Scan that matches any rowkey that is equal to the key, and that contains
     a date that is within the boundaries set inside the PrefixAndTimeBoundKeyValueMatchingStrategy.
     Example: key to delete = "k1", start = 201910100000, end = 201910202359, separator = "|", pattern = "yyyMMddHHmm"
              rowkey = "k1|201910152256" is matched */
  private def scanPrefixWithTime(config: Config,
                                 keysToDelete: RDD[RowKeyWithCorrelation],
                                 matchingStrategy: PrefixAndTimeBoundKeyValueMatchingStrategy): RDD[(RowKeyWithCorrelation, Scan)] = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(matchingStrategy.pattern, new Locale(matchingStrategy.locale))

    val startDate = wrapConfigException(config.getLong(START_PERIOD_KEY))
    val endDate = wrapConfigException(config.getLong(END_PERIOD_KEY))
    val zoneId = try {
      ZoneId.of(config.getString(TIMEZONE_PERIOD_KEY))
    } catch {
      case _: ConfigException.Missing => ZoneId.of("UTC")
    }
    val startDateString = formatMillis(formatter, startDate, zoneId)
    val endDateString = formatMillis(formatter, endDate, zoneId)
    val startDateBytes = Bytes.toBytes(startDateString)
    val endDateBytes = Bytes.toBytes(endDateString)
    keysToDelete.map { case rowKeyWithCorrelation@RowKeyWithCorrelation(rowKey, _) =>
      val scan = new Scan()
      val startRow = rowKey ++ Bytes.toBytes(matchingStrategy.separator) ++ startDateBytes :+ 0x00.toByte
      val stopRow = rowKey ++ Bytes.toBytes(matchingStrategy.separator) ++ endDateBytes :+ 0xFF.toByte
      logger.info(s"Scan start: '${new String(startRow, StandardCharsets.UTF_8)}', scan stop: '${new String(stopRow, StandardCharsets.UTF_8)}'")
      scan.setStartRow(startRow)
      scan.setStopRow(stopRow)
      rowKeyWithCorrelation -> scan.setFilter(
        new FilterList(
          new KeyOnlyFilter()
        )
      )
    }
  }

  private def formatMillis(formatter: DateTimeFormatter, millis: Long, zoneId: ZoneId): String = {
    val instant = Instant.ofEpochMilli(millis)
    val zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId)
    formatter.format(zonedDateTime)
  }

  private def wrapConfigException[T](value: => T): T = try {
    value
  } catch {
    case e: ConfigException => throw KeyValueConfigException(e, "Missing mandatory configuration key`")
  }
}

