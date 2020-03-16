package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs

import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.GdprUtils
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.hdfs.HdfsUtils
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

import scala.util.{Success, Try}

/**
  * Handler for the backup operations of HDFS files
  * @param fs FileSystem to use for the operations
  * @param backupParentDirPath Base path where to create the backup directory
  * @param dataPath Path containing the data that will be backup
  */
class HdfsBackupHandler(fs: FileSystem, backupParentDirPath: Path, dataPath: Path) {

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
    *     `backupDir` = "/user/backup_123"
    *  - then it copies the file into this directory, replacing the prefix "/user/data" with "/user/backup_123":
    *      "/user/backup_123/p1=a/p2=b/file.parquet"
    *
    * @param filesToBackup Files that should be copied in the backup directory
    * @return Path of the newly created backup directory
    */
  def backup(filesToBackup: Seq[Path]): Try[Path] = {
    HdfsUtils.backupFiles(fs)(filesToBackup, backupParentDirPath, dataPath)
  }

  /**
    * Restores files backup inside `backupPath` to `dataPath`.
    * Each of the files listed recursively inside `backupPath` is "moved back" to the data directory,
    * by replacing the prefix of the backup directory with the prefix of the data directory.
    * Example:
    * `backupPath` = "/user/backup_123"
    * `backupParentDir` = "/user"
    * `dataPath` = "/user/data"
    *
    * - Listed backup file
    *     "/user/backup_123/p1=a/p2=b/file.parquet"
    * - this file is renamed by replacing the prefix "/user/backup_123" with "/user/data"
    *    "/user/data/p1=a/p2=b/file.parquet"
    *
    * @param backupPath Path of the backup directory containing the files to restore
    */
  def restoreBackup(backupPath: Path): Try[Unit] = {
    for {
      iterator <- Try(fs.listFiles(backupPath, true))
      // Rename all files inside the backup dir. If only one of the renames fails, the entire restoring process fails
      renameResult <- HdfsUtils.foldIterator(iterator, Success(true))(renameAndAccumulate(backupPath))(b => !b)
      _ <- GdprUtils.recoverFsOperation(renameResult, "Error while moving files from backup dir to data dir")
    } yield ()
  }

  /**
    * Delete the entire backup directory
    * @param backupPath Path of the backup directory to delete
    */
  def deleteBackup(backupPath: Path): Try[Unit] = {
    HdfsUtils.deletePath(fs)(backupPath)
  }

  /* Moves `file` from backup dir to data dir by removing the prefix `backupPath` and replacing it with `dataPath`.
     The result is calculated as `acc` && `renameResult`*/
  private def renameAndAccumulate(backupPath: Path)
                                 (acc: Boolean, file: LocatedFileStatus): Boolean = {
    if (!file.isDirectory) {
      val newPath = HdfsUtils.replacePathPrefix(file.getPath, backupPath, dataPath)
      fs.rename(file.getPath, newPath) && acc
    } else {
      acc
    }
  }

}
