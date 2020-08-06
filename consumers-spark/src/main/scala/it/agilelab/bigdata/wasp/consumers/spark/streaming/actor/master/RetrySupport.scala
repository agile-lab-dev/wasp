package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.ActorLogging

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

trait RetrySupport {
  self: ActorLogging =>

  trait Recoverable[F[_]] {
    def recoverWith[A, B >: A](self: F[A])(e: Throwable => F[B]): F[B]

    def extractor[A](self: F[A]): A
  }

  implicit def recoverableTry: Recoverable[Try] = new Recoverable[Try] {
    override def recoverWith[A, B >: A](self: Try[A])(e: Throwable => Try[B]): Try[B] =
      self.recoverWith {
        case ex => e(ex)
      }

    override def extractor[A](self: Try[A]): A = self.get
  }

  implicit def recoverableFuture(implicit ec: ExecutionContext): Recoverable[Future] = new Recoverable[Future] {
    override def recoverWith[A, B >: A](self: Future[A])(e: Throwable => Future[B]): Future[B] =
      self.recoverWith {
        case ex => e(ex)
      }

    override def extractor[A](self: Future[A]): A =
      Await.result(self, Duration.Inf)
  }

  def retry[F[_] : Recoverable, A](retryInterval: FiniteDuration)(retryable: () => F[A]): A = {

    val Recoverable: Recoverable[F] = implicitly[Recoverable[F]]

    @tailrec
    def inner(maybeError: Option[Throwable]): A = {

      maybeError.foreach { e =>
        Thread.sleep(retryInterval.toMillis)
        log.warning("Retry ciccio {}", e.getMessage, e)
      }

      try {
        Recoverable.extractor(retryable())
      } catch {
        case e: Exception =>
          inner(Some(e))
      }

    }

    inner(None)
  }

}
