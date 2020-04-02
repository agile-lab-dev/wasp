package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs

import com.github.dwickern.macros.NameOf.nameOf
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HdfsDeletionConfig
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.exception._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.HdfsDataDeletion._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, expr, input_file_name}
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import scala.util.{Failure, Success, Try}

class HdfsDataDeletion(fs: FileSystem) extends Logging {
  def delete(config: HdfsDeletionConfig, spark: SparkSession): Try[Seq[DeletionOutput]] = {
    logger.info("Starting HDFS deletion handling")
    val handler = new HdfsDeletionHandler(fs, config, spark)
    val dataPath = new Path(config.rawModel.uri)
    val backupHandler = new HdfsBackupHandler(fs, new Path(config.backupDirUri), dataPath)

    val output = if (config.keysToDeleteWithCorrelation.nonEmpty) {
      delete(handler, backupHandler, config, dataPath, spark)
    } else {
      logger.info(s"No keys to delete, completing deletion successfully with no output")
      Success(Seq.empty)
    }

    output match {
      case Failure(_) => logger.info("Deletion failed")
      case Success(_) => logger.info("Deletion completed successfully")
    }
    output
  }

  def delete(deletionHandler: HdfsDeletionHandler,
             backupHandler: HdfsBackupHandler,
             config: HdfsDeletionConfig,
             dataPath: Path,
             spark: SparkSession): Try[Seq[DeletionOutput]] = {
    for {
      filesWithKeys <- if (config.missingPathFailure || fs.exists(new Path(config.rawModel.uri))) {
        // if rawModel read can fail for a missing path or we're sure that the path exists then read
        getFilesToFilter(config, spark)
      } else {
        // if rawModel read cannot fail then return an empty Array
        Success(
          config.keysToDeleteWithCorrelation.map{ keyWithCorrelation =>
            (None, keyWithCorrelation)
          }.toArray[(Option[String], KeyWithCorrelation)]
        )
      }
      files = filesWithKeys.collect { case (Some(fileName), _) => fileName }.distinct.toList
      backupDir <- backup(backupHandler, files.map(new Path(_)))
      _ <- deleteOrRollback(deletionHandler, files, backupHandler, backupDir, dataPath)
      _ <- if (files.nonEmpty) {
        deleteBackup(backupHandler, backupDir)
      } else {
        Success(())
      }
    } yield createOutput(config, filesWithKeys)
  }

  private def backup(backupHandler: HdfsBackupHandler, pathsToBackup: Seq[Path]): Try[Path] = {
    backupHandler.backup(pathsToBackup) match {
      case Failure(exception) => Failure(BackupException(exception))
      case Success(backupPath) =>
        logger.info(s"Successfully backup files to $backupPath")
        Success(backupPath)
    }
  }

  /* Handle `filesToHandle` using `hdfsDeletionHandler`. In case of a failure during handling, it tries to restore
     the backup stored in `backupDir` using `backupHandler` */
  private def deleteOrRollback(hdfsDeletionHandler: HdfsDeletionHandler,
                               filesToHandle: List[String],
                               backupHandler: HdfsBackupHandler,
                               backupDir: Path,
                               dataPath: Path): Try[Unit] = {
    logger.info(s"Performing handling of files found...")
    hdfsDeletionHandler.delete(filesToHandle) recoverWith {
      case deletionThrowable =>
        logger.error(s"Error during deletion of model inside $dataPath. Restoring backup from $backupDir...")
        val deletionException = DeletionException(deletionThrowable)
        backupHandler.restoreBackup(backupDir) match {
          case Failure(restorationThrowable) =>
            logger.error(s"ILLEGAL STATE: exception during backup restoration of $backupDir to $dataPath after deletion failure.")
            Failure(FailureDuringBackupRestorationException(deletionException, restorationThrowable))
          case Success(_) =>
            logger.info(s"Successfully restored backup of $dataPath.")
            backupHandler.deleteBackup(backupDir) match {
              case Failure(exception) =>
                logger.error(s"Error while deleting backup '$backupDir' after successful backup restoration")
                Failure(FailureDuringBackupDeletionAfterRestorationException(deletionException, exception))
              case Success(_) =>
                logger.info(s"Successfully deleted backup.")
                Failure(deletionException)
            }
        }
    }
  }

  private def deleteBackup(backupHandler: HdfsBackupHandler, backupPath: Path): Try[Unit] = {
    logger.info(s"Deletion performed correctly. Deleting backup directory '$backupPath'...")
    backupHandler.deleteBackup(backupPath).recoverWith {
      case t =>
        logger.error(s"Error during deletion of backup dir $backupPath")
        Failure(BackupDeletionException(t))
    }
  }

  /* Retrieve all files to filter, together with the keys to delete each file contains */
  private def getFilesToFilter(config: HdfsDeletionConfig, spark: SparkSession): Try[Array[(Option[FileName], KeyWithCorrelation)]] = {
    logger.info(s"Searching for files containing the selected keys...")
    import spark.implicits._
    val tryFiles = for {
      rawDataDF <- HdfsUtils.readRawModel(config.rawModel, spark)
      // Retrieve all the files that contain at least on of the key to delete
      filesToFilterAndKeys <- filterDataFrame(config, rawDataDF)
      _ = logger.info(s"Files to filter: ${filesToFilterAndKeys.collect{ case (Some(x), _) => x }.mkString("\n", "\n", "")}")
    } yield filesToFilterAndKeys

    tryFiles.recoverWith {
      case t => Failure(DeletionException(t))
    }
  }

  /* Filter `rawDataDF` with the RawMatchingStrategy and PartitionPruningStrategy defined in the config,
     returning the files to be handled and the list of keys matched to delete that each of them contains */
  private def filterDataFrame(config: HdfsDeletionConfig, rawDataDF: DataFrame)
                             (implicit ev: Encoder[(Option[String], String, String)]): Try[Array[(Option[FileName], KeyWithCorrelation)]] = {
    Try {
      import org.apache.spark.sql.functions.broadcast
      val broadcastDF = broadcast(rawDataDF.sparkSession.createDataFrame(config.keysToDeleteWithCorrelation))

      rawDataDF
        .select(
          input_file_name().alias(FILENAME_COLUMN),
          expr(config.rawMatchingStrategy.dataframeKeyMatchingExpression).alias(DATA_KEY_COLUMN)
        )
        .where(config.partitionPruningCondition)
        .join(
          broadcastDF,
          config.joinCondition(col(DATA_KEY_COLUMN), broadcastDF(nameOf[KeyWithCorrelation](_.key))),
          "right_outer"
        )
        .select(
          col(FILENAME_COLUMN),
          broadcastDF(nameOf[KeyWithCorrelation](_.key)),
          broadcastDF(nameOf[KeyWithCorrelation](_.correlationId))
        )
        .distinct()
        .as[(Option[FileName], String, String)]
        .collect()
        .map {
          case (maybeFile, key, corrId) => (maybeFile, KeyWithCorrelation(key, corrId))
        }
    }

  }

  private def createOutput(config: HdfsDeletionConfig,
                           filesFilteredAndKeys: Array[(Option[FileName], KeyWithCorrelation)]): Seq[DeletionOutput] = {
    val matchType: HdfsMatchType = HdfsMatchType.fromRawMatchingStrategy(config.rawMatchingStrategy)

    filesFilteredAndKeys.groupBy(_._2).map {
      case (keyWithCorrelation, fileNames) => fileNames match {
        case Array((None, _)) =>
          // not found
          DeletionOutput(keyWithCorrelation, matchType, HdfsRawModelSource(config.rawModel.uri), DeletionNotFound)
        case f =>
          val files = f.collect { case (Some(fileName), _) => fileName }
          DeletionOutput(keyWithCorrelation, matchType, HdfsFileSource(files), DeletionSuccess)
      }
    }.toSeq
  }

}

object HdfsDataDeletion {
  type KeyName = String
  type FileName = String
  type KeysMatchedToDelete = List[String]
  type FilesToDelete = List[FileName]
  val FILENAME_COLUMN = "fileName"
  val DATA_KEY_COLUMN = "dataKeyColumn"
}

case class HdfsDeletionResult(keyToDeleteWithCorrelation: KeyWithCorrelation, keyMatched: String, fileName: FileName)