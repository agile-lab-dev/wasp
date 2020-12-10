package it.agilelab.bigdata.wasp.consumers.spark.http.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy protected implicit val spark: SparkSession =
    SparkSessionTestWrapper.spark.newSession()

}

object SparkSessionTestWrapper {
  private val warehouseLocation = f"file:$${system:user.dir}/spark-warehouse"
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

