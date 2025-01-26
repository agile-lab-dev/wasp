package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

import java.util.concurrent.atomic.AtomicInteger

class TestStuckQueryStrategy extends Strategy {

  /**
    * Adds static headers to the first input DataFrame and returns the result.
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    implicit val rowEncoder: ExpressionEncoder[Row] = RowEncoder(dataFrames.head._2.schema)

    dataFrames.head._2.mapPartitions { it =>
      if (TaskContext.getPartitionId() == 0 && TestStuckQueryStrategy.counter.getAndIncrement() % 2 == 1) {
        Thread.sleep(100000)
      }

      it
    }

  }
}

object TestStuckQueryStrategy {
  lazy val counter = new AtomicInteger(0)
}
