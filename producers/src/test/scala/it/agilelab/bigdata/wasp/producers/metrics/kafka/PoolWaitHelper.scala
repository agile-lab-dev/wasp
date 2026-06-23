package it.agilelab.bigdata.wasp.producers.metrics.kafka

import org.scalatest.Assertions
import scala.annotation.tailrec

trait PoolWaitHelper extends Assertions {

  @tailrec
  final def awaitPoolEntry(key: String, deadlineMs: Long = System.currentTimeMillis() + 30000): Unit = {
    if (!Constants.offsetCheckerPool.contains(key)) {
      if (System.currentTimeMillis() > deadlineMs)
        fail(s"Timed out waiting for offsetCheckerPool entry '$key'")
      Thread.sleep(10)
      awaitPoolEntry(key, deadlineMs)
    }
  }
}
