package it.agilelab.bigdata.wasp.core.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/**
  * Utility trait to mix in test suites.
  *
  * This trait is not declared among tests because test dependencies
  * are not transitively resolved by sbt
  */
trait SparkTestKit {
  lazy val sc: SparkContext = ss.sparkContext
  lazy val ss: SparkSession = SparkTestKit._ss
}


/**
  * This object will hold a singleton and per jvm spark context
  * (sbt will run test in parallel and spark has a
  * "one spark context per jvm restriction")
  */
object SparkTestKit {
  private lazy val _ss: SparkSession = {
    SparkSession.builder()
      .appName("wasp-test-kit")
      .master("local[*]")
      .getOrCreate()
  }
}
