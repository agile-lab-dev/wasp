package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{HasPostMaterializationHook, ReaderKey, Strategy}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class TestIdentityStrategyPostHook extends Strategy with HasPostMaterializationHook {

  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    println(s"Strategy configuration: $configuration")

    dataFrames.head._2
  }

  /**
    * Hook called by the wasp framework after the write phase finishes
    *
    * @param maybeError Some(error) if an error happened during materialization; None otherwise
    */
  override def postMaterialization(maybeDataframe: Option[DataFrame], maybeError: Option[Throwable]): Try[Unit] = {
    Seq.range(0, 100).foreach(_ => println("---------------------HOOKCALLED-----------------------"))

    Try(new Exception("Failing hook"))
  }
}