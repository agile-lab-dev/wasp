package it.agilelab.bigdata.wasp.consumers.spark.eventengine

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * This trait allows to use spark in Unit tests
  * https://stackoverflow.com/questions/43729262/how-to-write-unit-tests-in-spark-2-0
  */
trait SparkSetup {

  private lazy val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Spark test")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def withSparkSession(testMethod: SparkSession => Any): Unit = {
    System.setSecurityManager(null)
    testMethod(sparkSession)
    // finally sparkSession.stop()
  }
}
