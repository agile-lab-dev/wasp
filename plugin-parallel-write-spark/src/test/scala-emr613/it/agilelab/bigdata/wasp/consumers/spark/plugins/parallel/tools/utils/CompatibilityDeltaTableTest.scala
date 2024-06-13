package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import org.apache.spark.sql.SparkSession

object CompatibilityDeltaTableTest {
  def configureSparkSession(ss: SparkSession) =
    SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config(ss.conf.getAll)
      .getOrCreate()

}
