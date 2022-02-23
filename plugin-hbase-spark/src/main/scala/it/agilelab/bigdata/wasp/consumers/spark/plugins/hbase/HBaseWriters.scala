package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import java.io.File

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.repository.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.KeyValueModel
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.spark.{HBaseContext, PutConverterFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql._

object HBaseBatchWriter {
  def createSparkStructuredStreamingWriter(keyValueBL: KeyValueBL, ss: SparkSession, hbaseModel: KeyValueModel): SparkStructuredStreamingWriter = {
    new HBaseStructuredStreamingWriter(hbaseModel, ss)
  }

  def createSparkWriter(keyValueBL: KeyValueBL, sc: SparkContext, hbaseModel: KeyValueModel): SparkBatchWriter = {
    new HBaseBatchWriter(hbaseModel, sc)
  }
}

class HBaseStructuredStreamingWriter(hbaseModel: KeyValueModel,
                                     ss: SparkSession)
  extends SparkStructuredStreamingWriter {
  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val options: Map[String, String] =
      hbaseModel.getOptionsMap ++
      hbaseModel.avroSchemas.getOrElse(Map()) ++
      Seq(
        HBaseTableCatalog.tableCatalog -> hbaseModel.tableCatalog,
        KeyValueModel.metadataAvroSchemaKey -> KeyValueModel.metadataAvro,
        HBaseTableCatalog.newTable -> "4",
        "useAvroSchemaManager" -> hbaseModel.useAvroSchemaManager.toString
      )
    val convertedStream = PutConverterFactory.convertAvroColumns(options, stream)
    convertedStream.writeStream
      .options(options)
      .format("org.apache.hadoop.hbase.spark")
  }
}

class HBaseBatchWriter(hbaseModel: KeyValueModel,
                       sc: SparkContext)
  extends SparkBatchWriter {

  override def write(df: DataFrame): Unit = {

    val options: Map[String, String] = hbaseModel.getOptionsMap ++
    hbaseModel.avroSchemas.getOrElse(Map()) ++
    Seq(
      HBaseTableCatalog.tableCatalog -> hbaseModel.tableCatalog,
      KeyValueModel.metadataAvroSchemaKey -> KeyValueModel.metadataAvro,
      HBaseTableCatalog.newTable -> "4",
      "useAvroSchemaManager" -> hbaseModel.useAvroSchemaManager.toString
    )

    df.write
      .options(options)
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
}