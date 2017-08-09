package it.agilelab.bigdata.wasp.consumers.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

/* Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}