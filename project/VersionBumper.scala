import BranchingModelSupport.BaseVersion
import sbt.Logger
import sbt.io.IO
import java.io.File
import scala.util.Try
import scala.util.Failure


object VersionBumper {

  def bumpMajor(v: String, log: Logger): BaseVersion = {
    bump(v, _.bumpMajor(), log)
  }

  def bumpMinor(v: String, log: Logger): BaseVersion = {
    bump(v, _.bumpMinor(), log)
  }

  def bumpPatch(v: String, log: Logger): BaseVersion = {
    bump(v, _.bumpPatch(), log)
  }

  private def bump(v: String, f: BaseVersion => BaseVersion, log: Logger): BaseVersion = {
    BaseVersion
      .parse(v)
      .map { parsedVersion =>
        log.info("Current base version: " + parsedVersion)
        val nextV = f(parsedVersion)
        log.info("Bumping to version: " + nextV)
        nextV
      }
      .flatMap(replaceBaseVersion(_, log))
      .fold(t => throw t, nextV => {
        log.success("Bumped to version: " + nextV)
        log.warn("Please reload sbt shell to apply the new version!")
        nextV
      })
  }

  private def replaceBaseVersion(parsedVersion: BaseVersion, log: Logger): Either[Throwable, BaseVersion] = {
    val officialFile = new File("baseVersion.version")
    val newFile      = new File("baseVersion.version.new")
    val backupFile   = new File("baseVersion.version.bak")

    def backupFileOrRecover[A](a: A) =
      Try {
        IO.copyFile(officialFile, backupFile)
        log.info(s"Backupped up file from $officialFile to $backupFile")
      }.recoverWith {
        case t: Throwable =>
          Try {
            log.err(s"Failed to backup $officialFile into $backupFile")
            log.err(s"Deleting newly generated $newFile")
            IO.delete(newFile)
            log.err(s"Deleting backup file generated $backupFile")
            IO.delete(backupFile)
          }.flatMap(_ => Failure(t))
      }
    def overwriteNewVersionOrRecover[A](a: A) =
      Try {
        IO.copyFile(newFile, officialFile)
        log.success(s"Overwritten ${officialFile} with ${newFile}")
      }.recoverWith {
        case t: Throwable =>
          Try {
            log.err(s"Failed to overwrite ${officialFile} with ${newFile}")
            log.err(s"Performing recover of ${backupFile} over ${officialFile}")
            IO.copyFile(backupFile, officialFile)
            log.err(s"Backup restored successfully")
          }.flatMap(_ => Failure(t))
      }

    rewriteFile(parsedVersion, officialFile, newFile)
      .map(_ => log.info("Written new version to file: " + newFile))
      .flatMap(backupFileOrRecover)
      .flatMap(overwriteNewVersionOrRecover)
      .Finally {
        log.info(s"Cleaning up ${backupFile} and ${newFile}")
        IO.delete(backupFile)
        IO.delete(newFile)
        log.info(s"Cleaned up ${backupFile} and ${newFile}")
      }
      .toEither
      .map(_ => parsedVersion)
  }

  private def rewriteFile(parsedVersion: BaseVersion, officialFile: File, newFile: File): Try[Unit] = {
    Try {
      IO.write(newFile, parsedVersion.majorMinorPatch)
    }
  }

  implicit class TryHasFinally[T](val value: Try[T]) extends AnyVal {
    import scala.util.control.NonFatal

    def Finally(action: => Unit): Try[T] =
      try {
        action;
        value
      } catch {
        case NonFatal(cause) => Failure[T](cause)
      }
  }
}
