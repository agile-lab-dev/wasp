package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.net.URI

trait DeltaParallelWriterTrait extends ColdParallelWriter {

  override final def performColdWrite(df: DataFrame, path: URI, partitioningColumns: Seq[String], batchId: Long): Unit = {
    performDeltaWrite(df, path, partitioningColumns, batchId)
    reconciliateManifest(getDeltaTable(path, df.sparkSession, partitioningColumns))
  }

  protected def performDeltaWrite(df: DataFrame, path: URI, partitioningColumns: Seq[String], batchId: Long): Unit

  private def reconciliateManifest(deltaTable: DeltaTable): Unit =
    deltaTable.generate("symlink_format_manifest")

  protected def getDeltaTable(path: URI, spark: SparkSession, partitions: Seq[String]): DeltaTable = {
    if (DeltaTableUtils.isDeltaTable(spark, new Path(path))) {
      DeltaTable.forPath(spark, path.toString)
    } else {
      createDelta(path, spark, partitions)
    }
  }

  private def createDelta(path: URI, spark: SparkSession, partitions: Seq[String]): DeltaTable = {
    val schema = catalogService.getSchema(spark, entityDetails)
    if (path.getScheme == "file") {
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
        schema)
      emptyDF.write.partitionBy(partitions: _*).format("delta").save(path.toString)
      DeltaTable.forPath(spark, path.toString)
    }  else  {
      throw new Exception(s"$path does not refer to a delta table nor to a local file path")
    }
  }
}


