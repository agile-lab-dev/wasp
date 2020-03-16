package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.exception

sealed trait GdprException

case class BackupException(throwable: Throwable)
  extends Exception(throwable.getLocalizedMessage, throwable)
    with GdprException

case class DeletionException(throwable: Throwable)
  extends Exception(throwable.getLocalizedMessage, throwable)
    with GdprException

case class BackupDeletionException(throwable: Throwable)
  extends Exception(throwable.getLocalizedMessage, throwable)
    with GdprException

case class BackupRestorationException(throwable: Throwable)
  extends Exception(throwable.getLocalizedMessage, throwable)
    with GdprException

case class FailureDuringBackupRestorationException(deletionException: DeletionException, restorationThrowable: Throwable)
  extends Exception(restorationThrowable.getLocalizedMessage, restorationThrowable.initCause(deletionException))
    with GdprException

case class FailureDuringBackupDeletionAfterRestorationException(deletionException: DeletionException, backupDeletionThrowable: Throwable)
  extends Exception(backupDeletionThrowable.getLocalizedMessage, backupDeletionThrowable.initCause(deletionException))
    with GdprException
