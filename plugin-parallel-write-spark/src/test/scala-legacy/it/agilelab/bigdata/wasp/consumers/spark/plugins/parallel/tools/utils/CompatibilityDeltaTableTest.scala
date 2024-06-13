package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import org.apache.spark.sql.SparkSession

object CompatibilityDeltaTableTest {
  def configureSparkSession(ss: SparkSession) = ss
}
