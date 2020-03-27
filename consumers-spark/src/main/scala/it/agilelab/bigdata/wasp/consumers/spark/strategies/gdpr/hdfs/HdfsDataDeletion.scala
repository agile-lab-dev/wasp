package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HdfsDeletionConfig
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.exception._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.HdfsDataDeletion._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{ExactRawMatchingStrategy, PrefixRawMatchingStrategy}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{expr, input_file_name}
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import scala.util.{Failure, Success, Try}

class HdfsDataDeletion(fs: FileSystem) extends Logging {
  def delete(config: HdfsDeletionConfig, spark: SparkSession): Try[Seq[DeletionOutput]] = {
    val handler = new HdfsDeletionHandler(fs, config, spark)
    val dataPath = new Path(config.rawModel.uri)
    val backupHandler = new HdfsBackupHandler(fs, new Path(config.backupDirUri), dataPath)

    delete(handler, backupHandler, config, dataPath, spark)
  }

  def delete(deletionHandler: HdfsDeletionHandler,
             backupHandler: HdfsBackupHandler,
             config: HdfsDeletionConfig,
             dataPath: Path,
             spark: SparkSession): Try[Seq[DeletionOutput]] = {
    for {
      filesWithKeys <- getFilesToFilter(config, spark)
      files = filesWithKeys.keySet.toList
      backupDir <- backup(backupHandler, files.map(new Path(_)))
      _ <- deleteOrRollback(deletionHandler, files, backupHandler, backupDir, dataPath)
      _ <- if (files.nonEmpty) {
        deleteBackup(backupHandler, backupDir)
      } else {
        Success(())
      }
      output <- createOutput(config, filesWithKeys)
    } yield output
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
    logger.info(s"Performing handling of parquet files found...")
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

  /* Retrieve all parquet files to filter, together with the keys to delete each file contains */
  private def getFilesToFilter(config: HdfsDeletionConfig, spark: SparkSession): Try[Map[FileName, KeysMatchedToDelete]] = {
    logger.info(s"Searching for parquet files containing the selected keys...")
    import spark.implicits._
    val tryFiles = for {
      rawDataDF <- HdfsUtils.readRawModel(config.rawModel, spark)
      // Retrieve all the parquet files that contain at least on of the key to delete
      filesToFilterAndKeys <- filterDataFrame(config, rawDataDF)
      _ = logger.info(s"Files to filter: ${filesToFilterAndKeys.mkString("\n", "\n", "")}")
    } yield filesToFilterAndKeys

    tryFiles.recoverWith {
      case t => Failure(DeletionException(t))
    }
  }

  /* Filter `rawDataDF` with the RawMatchingStrategy and PartitionPruningStrategy defined in the config,
     returning the parquet files to be handled and the list of keys matched to delete that each of them contains */
  private def filterDataFrame(config: HdfsDeletionConfig, rawDataDF: DataFrame)
                             (implicit ev: Encoder[(String, String)]): Try[Map[FileName, KeysMatchedToDelete]] = {
    Try {
      rawDataDF
        .select(input_file_name(), expr(config.rawMatchingStrategy.dataframeKeyMatchingExpression))
        .where(config.rawMatchingCondition.and(config.partitionPruningCondition))
        .as[(FileName, KeyName)]
        .collect()
        // group by filename
        .groupBy(_._1)
        .mapValues { array =>
          array.map {
            case (_, key) => key
          }.toList
        }
    }

  }

  private def createOutput(config: HdfsDeletionConfig,
                           filesFilteredAndKeys: Map[FileName, KeysMatchedToDelete]): Try[Seq[DeletionOutput]] = Try {
    val deletionResults: Iterable[HdfsDeletionResult] = filesFilteredAndKeys.flatMap {
      case (fileName, keysMatchedToDelete) =>
        keysMatchedToDelete.map { keyMatched =>
          val keyToMatch = config.keysToDeleteWithCorrelation.find(keyToDelete => keyMatched.startsWith(keyToDelete.key))
              .getOrElse(throw new IllegalStateException(s"Cannot find key matched '$keyMatched' among keys submitted to the job to be matched"))
          HdfsDeletionResult(keyToMatch, keyMatched, fileName)
        }
    }

    val keysDeletedOutput: Seq[DeletionOutput] = config.rawMatchingStrategy match {
      case ExactRawMatchingStrategy(dataframeKeyMatchingExpression) =>
        deletionResults.map { case HdfsDeletionResult(keyToDeleteWithCorrelation, _, fileName) =>
          DeletionOutput(keyToDeleteWithCorrelation, HdfsExactColumnMatch(dataframeKeyMatchingExpression), HdfsParquetSource(Seq(fileName)), DeletionSuccess)
        }.toSeq
      case PrefixRawMatchingStrategy(dataframeKeyMatchingExpression) =>
        deletionResults.groupBy { case HdfsDeletionResult(keyToDeleteWithCorrelation, _, _) => keyToDeleteWithCorrelation }.map {
          case (keyToDeleteWithCorrelation, results) =>
            val keysMatched = results.map(_.keyMatched).toSeq
            val fileNames = results.map(_.fileName).toSeq
            DeletionOutput(keyToDeleteWithCorrelation, HdfsPrefixColumnMatch(dataframeKeyMatchingExpression, Some(keysMatched)), HdfsParquetSource(fileNames), DeletionSuccess)
        }.toSeq
    }

    val keysNotFoundOutput: Seq[DeletionOutput] = config.keysToDeleteWithCorrelation
      .filterNot { keyToDeleteWithCorrelation =>
        val keysDeleted = keysDeletedOutput.map(_.key)
        config.rawMatchingStrategy match {
          case _: ExactRawMatchingStrategy => keysDeleted.contains(keyToDeleteWithCorrelation.key)
          case _: PrefixRawMatchingStrategy => keysDeleted.exists(_.startsWith(keyToDeleteWithCorrelation.key))
        }
      }.map { keyWithCorrelation =>
        val keyMatchType = config.rawMatchingStrategy match {
          case ExactRawMatchingStrategy(dataframeKeyMatchingExpression) => HdfsExactColumnMatch(dataframeKeyMatchingExpression)
          case PrefixRawMatchingStrategy(dataframeKeyMatchingExpression) => HdfsPrefixColumnMatch(dataframeKeyMatchingExpression, None)
        }
        DeletionOutput(keyWithCorrelation, keyMatchType, NoSourceFound, DeletionNotFound)
      }

    keysDeletedOutput ++ keysNotFoundOutput
  }

}

object HdfsDataDeletion {
  type KeyName = String
  type FileName = String
  type KeysMatchedToDelete = List[String]
  type FilesToDelete = List[FileName]
}

case class HdfsDeletionResult(keyToDeleteWithCorrelation: KeyWithCorrelation, keyMatched: String, fileName: FileName)