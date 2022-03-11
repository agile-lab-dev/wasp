package it.agilelab.bigdata.wasp.utils

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

object EitherUtils {

  def catchNonFatal[A](f: => A): Either[Throwable, A] =
    try {
      Right(f)
    } catch {
      case scala.util.control.NonFatal(t) => Left(t)
    }

  def traverse[L, R](list: List[Either[L, R]]): Either[L, List[R]] = {
    @tailrec
    def traverseR[L1, R1](tail: List[Either[L1, R1]], head: Either[L1, List[R1]]): Either[L1, List[R1]] =
      head match {
        case l @ Left(_) => l
        case r @ Right(z) =>
          tail match {
            case Nil            => r
            case Right(h) :: tl => traverseR(tl, Right(h :: z))
            case Left(b) :: _   => Left(b)
          }
      }
    traverseR(list, Right(List.empty)).map(_.reverse)
  }

  implicit class RightBiasedEither[A, B](val e: Either[A, B]) extends AnyVal {
    def flatMap[AA >: A, Y](f: B => Either[AA, Y]): Either[AA, Y] = e.right.flatMap(f)
    def map[Y](f: B => Y): Either[A, Y]                           = e.right.map(f)
  }

  implicit def eitherToTry[A](e: Either[Throwable, A]): Try[A] = e match {
    case Left(ex)     => Failure(ex)
    case Right(value) => Success(value)
  }

  implicit def tryToEither[A](t: Try[A]): Either[Throwable, A] = t match {
    case Failure(exception) => Left(exception)
    case Success(value)     => Right(value)
  }
}
