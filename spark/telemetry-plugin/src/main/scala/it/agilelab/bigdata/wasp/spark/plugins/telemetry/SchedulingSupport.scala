package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import java.time.Instant
import java.util.concurrent.{Executors, ScheduledFuture, ThreadFactory}

import scala.concurrent.duration.Duration

trait SchedulingSupport {

  private lazy val executorService = Executors.newScheduledThreadPool(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = Executors.defaultThreadFactory().newThread(r)
      t.setName("telemetry-plugin-thread")
      t.setDaemon(true)
      t
    }
  })

  def schedule(duration: Duration)(runnable: Instant => Unit): ScheduledFuture[_] = {

    val toRunnable = new Runnable {
      override def run(): Unit = try {
        runnable(Instant.now())
      } catch {
        case ex: Throwable => ex.printStackTrace()
      }
    }

    executorService.scheduleWithFixedDelay(toRunnable, 0, duration.length, duration.unit)
  }


}
