package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.utils.ConfigManagerHelper
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, RemoteIterator}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID
import scala.util.{Failure, Success, Try}

object HdfsUtils extends Logging {

  /**
    * Create a new directory inside `backupParentDir`, called "backup_{randomUUID}".
    * Each of the files inside `filesToBackup` will be copied in this directory, also maintaining
    * the eventual HDFS partitioning. The new file path is created by removing the base directory
    * (that is `dataPath`) from the file path, and replacing it with the path of the backup directory.
    * Example:
    * `filesToBackup` = ["/user/data/p1=a/p2=b/file.parquet"]
    * `backupParentDir` = "/user"
    * `dataPath` = "/user/data"
    *
    *  - This function creates:
    * `backupDir` = "/user/backup_123'
    *  - then it copies the file into this directory, replacing the prefix "/user/data" with "/user/backup_123":
    * "/user/backup_123/p1=a/p2=b/file.parquet"
    *
    * @param filesToBackup   Files that should be copied in the backup directory
    * @param backupParentDir Base path where to create the backup directory
    * @param dataPath        Path containing the data that will be backup
    * @return Path of the newly created backup directory
    */
  def backupFiles(fs: FileSystem)(filesToBackup: Seq[Path], backupParentDir: Path, dataPath: Path): Try[Path] = {
    val backupDirPath = new Path(backupParentDir, "backup" + "_" + UUID.randomUUID.toString)
    for {
      exists <- Try(fs.exists(backupDirPath))
      backupPath <- {
        if (exists) {
          Failure(new IllegalStateException(s"Backup directory '$backupDirPath' already exists!"))
        } else {
          if (filesToBackup.isEmpty) {
            logger.info(s"Nothing to backup, skipping: $dataPath")
            Success(backupDirPath)
          } else {
            logger.info(s"Backupping files ${filesToBackup.mkString("\n", "\n", "")} to '${backupDirPath.toString}'")
            for {
              moves <- GdprUtils.traverseTry(filesToBackup) { f =>
                        val newPath = replacePathPrefix(f, prefixPathToChange = dataPath, newPrefix = backupDirPath)
                        FileUtil.copy(fs, f, fs, newPath, false, fs.getConf)
                      }
              _ <- GdprUtils.recoverFsOperation(moves.forall(identity), s"Cannot copy files into '$backupDirPath")
            } yield backupDirPath
          }
        }
      }
    } yield backupPath
  }

  /* Replaces `prefixPathToChange` inside `filePath` with `newPrefix` */
  def replacePathPrefix(filePath: Path, prefixPathToChange: Path, newPrefix: Path): Path = {
    val fileUri              = filePath.toUri.getPath
    val fileUriWithoutPrefix = fileUri.removePrefix(prefixPathToChange.toUri.getPath)

    newPrefix.suffix(fileUriWithoutPrefix)
  }

  /* Finds all the directories of type "column=value" inside `uri` */
  def findPartitionColumns(uri: String): List[(String, String)] = {
    uri
      .split("/")
      .filter(_.contains("="))
      .map { partitionColumn =>
        val splits = partitionColumn.split("=")
        splits(0) -> splits(1)
      }
      .toList
  }

  def deletePath(fs: FileSystem)(sourcePath: Path): Try[Unit] = {
    /*    val iter = fs.listFiles(sourcePath, true)
        logger.info("deleting path that contains the following files: ")
        while (iter.hasNext) {
          logger.info(iter.next().getPath.toString)
        }*/
    logger.info(s"Deleting path: ${sourcePath.toUri.toString}")
    Try(fs.delete(sourcePath, true)).flatMap {
      case true  => Success(())
      case false => Failure(new IllegalStateException(s"Impossible to delete path ${sourcePath.toUri.toString}"))
    }
  }

  implicit class StringPrefix(string: String) {
    def removePrefix(prefix: String): String =
      if (string.startsWith(prefix)) {
        string.substring(prefix.length, string.length)
      } else {
        throw new IllegalArgumentException
      }
  }

  def getRawModelPathToWrite(rawModel: RawModel): String = {
    if (rawModel.timed) {
      // the path must be timed; add timed subdirectory
      val hdfsPath  = new Path(rawModel.uri)
      val timedPath = new Path(hdfsPath.toString + "/" + ConfigManagerHelper.buildTimedName("").substring(1) + "/")

      timedPath.toString
    } else {
      // the path is not timed; return it as-is
      rawModel.uri
    }
  }

  def getRawModelPathToToLoad(rawModel: RawModel, sc: SparkContext): String = {
    if (rawModel.timed) {
      // the path is timed; find and return the most recent subdirectory
      val hdfsPath = new Path(rawModel.uri)
      val hdfs     = hdfsPath.getFileSystem(sc.hadoopConfiguration)

      val subdirectories = hdfs
        .listStatus(hdfsPath)
        .toList
        .filter(_.isDirectory)
      val mostRecentSubdirectory = subdirectories.sortBy(_.getPath.getName).reverse.head.getPath

      mostRecentSubdirectory.toString
    } else {
      // the path is not timed; return it as-is
      rawModel.uri
    }
  }

  def readRawModel(rawModel: RawModel, spark: SparkSession): Try[DataFrame] = Try {
    // setup reader
    val schema: StructType = DataType.fromJson(rawModel.schema).asInstanceOf[StructType]
    val options            = rawModel.options
    val reader = spark.sqlContext.read
      .schema(schema)
      .format(options.format)
      .options(options.extraOptions.getOrElse(Map()))

    // calculate path
    val path = getRawModelPathToToLoad(rawModel, spark.sparkContext)

    logger.info(s"Load this path: '$path'")

    // read
    reader.load(path)
  }

  def writeRawModel(rawModel: RawModel, df: DataFrame): Try[Unit] = Try {
    logger.info(s"Initializing HDFS writer: $rawModel")

    // calculate path
    val path = getRawModelPathToWrite(rawModel)

    // get options
    val options      = rawModel.options
    val mode         = if (options.saveMode == "default") "error" else options.saveMode
    val format       = options.format
    val extraOptions = options.extraOptions.getOrElse(Map())
    val partitionBy  = options.partitionBy.getOrElse(Nil)

    // setup writer
    val writer = df.write
      .mode(mode)
      .format(format)
      .options(extraOptions)
      .partitionBy(partitionBy: _*)

    logger.info(s"Write in this path: '$path'")

    // write
    writer.save(path)

  }

  /* Folds `iterator` into a single value using `f`. `exitPath` is called for each new value of the accumulator,
       and if it returns true the iterations are stopped and the currently accumulated value is returned. */
  def foldIterator[T, B](iterator: RemoteIterator[T], acc: Try[B])(f: (B, T) => B)(exitPath: B => Boolean): Try[B] =
    acc match {
      case failure: Failure[B] => failure
      case Success(accOk) =>
        Try(iterator.hasNext).flatMap { hasNext =>
          if (hasNext && !exitPath(accOk)) {
            val newAcc = for {
              t    <- Try(iterator.next())
              newB <- Try(f(accOk, t))
            } yield newB
            foldIterator(iterator, newAcc)(f)(exitPath)
          } else {
            acc
          }
        }
    }

}
