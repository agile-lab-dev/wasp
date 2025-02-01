package it.agilelab.bigdata.wasp.consumers.spark.utils

import it.agilelab.bigdata.utils.FileSystemUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

import java.nio.file.Files

trait SparkSuite extends Suite {

  lazy val spark: SparkSession = {
    System.setSecurityManager(null)
    SparkSuite.spark.newSession()
  }

}

object SparkSuite {
  private val warehouseDir      = Files.createTempDirectory("spark-warehouse")
  private val warehouseLocation = warehouseDir.toUri.toString

  private val deltaSparkSessionExtensionClassname = "io.delta.sql.DeltaSparkSessionExtension"
  private val deltaCatalogClassname               = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

  private lazy val spark = {
    val builder = SparkSession
      .builder()
      .appName("test")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")

    try {
      Class.forName(deltaCatalogClassname)
      Class.forName(deltaSparkSessionExtensionClassname)
      builder.config("spark.sql.extensions", deltaSparkSessionExtensionClassname)
      builder.config("spark.sql.catalog.spark_catalog", deltaCatalogClassname)
    } catch {
      case _: ClassNotFoundException =>
    }
    val ss = builder.getOrCreate()
    sys.addShutdownHook {
      FileSystemUtils.recursivelyDeleteDirectory(warehouseDir)
      ss.close()
    }
    ss
  }
}
