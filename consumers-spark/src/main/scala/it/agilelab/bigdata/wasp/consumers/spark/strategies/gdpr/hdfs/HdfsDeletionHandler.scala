package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.DeletionOutput
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HdfsDeletionConfig
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{ExactRawMatchingStrategy, PrefixRawMatchingStrategy}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.{Success, Try}

class HdfsDeletionHandler(fs: FileSystem, config: HdfsDeletionConfig, spark: SparkSession) extends Logging {
  val dataPath = new Path(config.rawModel.uri)
  val stagingPath = new Path(config.stagingDirUri)

  /**
    * Deletes the keys to delete stored in [[config]].`keysToDelete` from the files `filesToFilter`.
    * All files to filter are read and merged into a DataFrame, and then only the rows that do not match the
    * RawMatchingStrategy and PartitionPruningStrategy defined in [[config]] are written into a staging directory.
    * Once the new parquet files are correctly written into the staging directory, the original `filesToFilter` are deleted,
    * and the newly written files are moved to the data directory.
    */
  def delete(filesToFilter: List[String]): Try[Unit] = {
    filesToFilter match {
      case Nil => Success(Seq.empty[DeletionOutput]) // No file to change
      case files =>
        for {
          // Read all files into a single DataFrame
          dfToFilter <- Try(files.map(readParquetWithPartitionColumns(spark)).reduce(_ union _))
          // Select only the rows not to be deleted
          dfToWrite = filterRowsToMaintain(dfToFilter)
          // Write new filtered data into staging dir
          _ <- HdfsUtils.writeRawModel(config.rawModel.copy(uri = config.stagingDirUri), dfToWrite)
          // Delete original files containing keys to remove
          deletion <- GdprUtils.traverseTry(filesToFilter)(f => fs.delete(new Path(f), false))
          _ <- GdprUtils.recoverFsOperation(deletion.forall(identity), "Error during deletion")
          // Move each of the new file from the staging dir to the original data dir
          iterator <- Try(fs.listFiles(stagingPath, true))
          // Rename all files inside the backup dir. If only one of the renames fails, the entire restoring process fails
          renameResult <- HdfsUtils.foldIterator(iterator, Success(true))(renameAndAccumulate(stagingPath, dataPath))(b => !b)
          _ <- GdprUtils.recoverFsOperation(renameResult, "Error while moving files from staging dir to data dir")
          // Delete staging dir
          _ <- HdfsUtils.deletePath(fs)(stagingPath)
        } yield ()

    }
  }

  /* Returns only the rows that do not match the strategies set */
  private def filterRowsToMaintain(dfToFilter: DataFrame): DataFrame = {
    dfToFilter.where(!config.rawMatchingCondition.and(config.partitionPruningCondition))
  }

  /* Read a single parquet file, including the partition columns derived from its path inside the dataframe */
  private def readParquetWithPartitionColumns(spark: SparkSession)(uri: String): DataFrame = {
    val df = spark.read.parquet(uri)

    HdfsUtils.findPartitionColumns(uri).foldLeft(df) {
      case (accDf, (colName, colValue)) => accDf.withColumn(colName, lit(colValue))
    }
  }

  /* Moves `file` from staging dir to data dir by removing the prefix `stagingPath` and replacing it with `dataPath`.
     The result is calculated as `acc` && `renameResult`*/
  private def renameAndAccumulate(stagingPath: Path, dataPath: Path)
                                 (acc: Boolean, file: LocatedFileStatus): Boolean = {
    if (!file.isDirectory && file.getPath.getName != "_SUCCESS") {
      val newPath = HdfsUtils.replacePathPrefix(file.getPath, stagingPath, dataPath)
      fs.rename(file.getPath, newPath) && acc
    } else {
      acc
    }
  }


}
