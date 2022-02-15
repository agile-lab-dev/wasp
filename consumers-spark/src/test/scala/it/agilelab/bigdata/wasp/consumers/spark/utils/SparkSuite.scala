package it.agilelab.bigdata.wasp.consumers.spark.utils

import it.agilelab.bigdata.wasp.core.build.BuildInfo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.Suite

trait SparkSuite extends Suite {

  lazy val spark: SparkSession = {
    System.setSecurityManager(null)
    SparkSuite.spark.newSession()
  }

}

object SparkSuite {
  private val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
  private lazy val spark = {
    val builder = SparkSession
      .builder()
      .appName("test")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")

    val ss = hiveTestConfig(builder).getOrCreate()
    sys.addShutdownHook {
      ss.close()
    }
    ss
  }

  def hiveTestConfig(builder: SparkSession.Builder) = {
    if (BuildInfo.flavor.contains("EMR_2_12")) {
      builder
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.hive.metastore.version", "2.3.3")
        .config("spark.hadoop.datanucleus.schema.autoCreateTables", "true")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        .config("spark.sql.hive.metastore.jars", "maven")
    } else {
      builder
    }
  }
}
