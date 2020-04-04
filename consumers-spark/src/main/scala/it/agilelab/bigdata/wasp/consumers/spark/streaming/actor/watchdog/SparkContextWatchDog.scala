package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.watchdog

import akka.actor.{Actor, Props}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.SparkContext


class SparkContextWatchDog private(sc: SparkContext, failureAction: () => Unit) extends Actor with Logging {

  import SparkContextWatchDog._
  import scala.concurrent.duration._

  implicit val ec = context.system.dispatcher
  context.system.scheduler.schedule(0.seconds, 1.second, self, MonitorSparkContext)


  def waitForSparkContextToBeAvailable : Receive  = {
    case MonitorSparkContext if !sc.isStopped =>
      logger.info("Spark context came up, beginning watchdog")
      context.become(superviseSparkContext)
    case MonitorSparkContext if sc.isStopped =>
      logger.trace("spark context has not started yet, giving it some slack")
  }


  def superviseSparkContext: Receive = {
    case MonitorSparkContext if sc.isStopped =>
      logger.trace("Spark context is dead")
      failureAction()

    case MonitorSparkContext if !sc.isStopped =>
      logger.trace("Everything is fine, spark context is alive")
  }

  override def receive: Receive = superviseSparkContext
}

object SparkContextWatchDog extends Logging {

  case object MonitorSparkContext

  private def exitAction(exitCode: Int)(): Unit = {

    logger.error {
      """
        |Spark context has been stopped --- Due to spark restriction of one spark context for jvm (even
        |failed spark contexts) the only meaningful thing to do is to selfdestruct the spark-consumers-streaming
        |process and hope that we are running under an orchestrator (kubernetes) or a process supervisor (systemd
        |supervisord). Hopefully someone or something will restart this process and all pipegraph will happily
        |restart thanks to fault tolerance capabilities of wasp.
      """.stripMargin
    }
    System.exit(exitCode)
  }

  def exitingWatchdogProps(sc: SparkContext, exitCode: Int): Props = {
    Props(new SparkContextWatchDog(sc, exitAction(exitCode)))
  }

  def logAndDoNothingWatchdogProps(sc: SparkContext): Props = {
    Props(new SparkContextWatchDog(sc, () => ()))
  }


}
