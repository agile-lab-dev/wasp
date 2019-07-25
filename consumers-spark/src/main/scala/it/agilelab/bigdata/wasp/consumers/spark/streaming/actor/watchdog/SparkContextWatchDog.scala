package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.watchdog

import akka.actor.{Actor, Props}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier

import scala.collection.JavaConverters._

class SparkContextWatchDog private(sc: SparkContext, failureAction: () => Unit) extends Actor with Logging {

  import SparkContextWatchDog._
  import scala.concurrent.duration._

  implicit val ec = context.system.dispatcher
  context.system.scheduler.schedule(0.seconds, 1.second, self, MonitorSparkContext)
  context.system.scheduler.schedule(0.seconds, 1.second, self, MonitorHdfTokens)


  def waitForSparkContextToBeAvailable : Receive  = {
    case MonitorSparkContext if !sc.isStopped =>
      logger.info("Spark context came up, beginning watchdog")
      context.become(superviseSparkContext.orElse(superviseDelegationTokens))
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

  def superviseDelegationTokens : Receive = {
    case MonitorHdfTokens =>
      val identifiers = UserGroupInformation.getCurrentUser
        .getCredentials
        .getAllTokens
        .asScala
        .map(_.decodeIdentifier())

      logger.trace(s"all token identifiers : $identifiers")

      val filtered =  identifiers.filter(_.isInstanceOf[DelegationTokenIdentifier])
        .map(_.asInstanceOf[DelegationTokenIdentifier])

      logger.trace(s"filtered token identifiers : $filtered")

      val maybeExpiredToken = filtered.filter(_.getMaxDate < System.currentTimeMillis()).toVector



      maybeExpiredToken.foreach { expired =>
          logger.error(s"Delegation token is expired $expired")
      }

      if(maybeExpiredToken.nonEmpty){
        failureAction()
      }else {
        logger.trace("Everything is fine, delegation tokens are ok")
      }

  }

  override def receive: Receive = waitForSparkContextToBeAvailable
}

object SparkContextWatchDog extends Logging {

  case object MonitorSparkContext

  case object MonitorHdfTokens

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

