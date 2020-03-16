package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

trait SparkSuite extends Suite {

  lazy val spark: SparkSession = {
    SparkSuite.spark.newSession()
  }

}

object SparkSuite {
  private val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
  private lazy val spark = {
    val ss = SparkSession
      .builder()
      .appName("test")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    sys.addShutdownHook {
      ss.close()
    }
    ss
  }
}

