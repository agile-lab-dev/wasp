package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils

import java.nio.charset.StandardCharsets

import scala.util.{Failure, Success, Try}

object GdprUtils {

  /* Traverse `seq` with the function `f`, wrapping the result in a Try.
     If `f` throws an exception for one of the elements, it returns a failure. */
  def traverseTry[A, B](seq: Seq[A])(f: A => B): Try[Seq[B]] = {
    seq.foldLeft(Success(Seq.empty[B]): Try[Seq[B]]) { (acc: Try[Seq[B]], a: A) =>
      for {
        bs <- acc
        b <- Try(f(a))
      } yield bs :+ b
    }
  }

  /* Traverse `seq` with the function `f`, wrapping the result in a Try.
     If `f` throws an exception for one of the elements, it returns a failure. */
  def traverseWithTry[A, B](seq: Seq[A])(f: A => Try[B]): Try[Seq[B]] = {
    seq.foldLeft(Success(Seq.empty[B]): Try[Seq[B]]) { (acc: Try[Seq[B]], a: A) =>
      for {
        bs <- acc
        b <- f(a)
      } yield bs :+ b
    }
  }

  /**
    * Wrap the boolean result of a FileSystem operation inside a Try
    * @param result Result of the FileSystem operation
    * @param errorMessage Error message of the Exception in case `result` is false
    * @return Success(Unit) if `result` is true, Failure(Exception(errorString)) if false
    */
  def recoverFsOperation(result: Boolean, errorMessage: String): Try[Unit] = {
    if (result) {
      Success(())
    } else {
      Failure(new Exception(errorMessage))
    }
  }

  implicit class RowKeyToString(rowKey: Array[Byte]) {
    def asString: String = new String(rowKey, StandardCharsets.UTF_8)
  }

  implicit class StringToRowKey(string: String) {
    def asRowKey: Array[Byte] = string.getBytes(StandardCharsets.UTF_8)
  }

}
