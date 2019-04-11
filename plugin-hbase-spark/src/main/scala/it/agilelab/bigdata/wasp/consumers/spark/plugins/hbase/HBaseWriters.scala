package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import java.io.File

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.KeyValueModel
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
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object HBaseBatchWriter {
  def createSparkStructuredStreamingWriter(keyValueBL: KeyValueBL, ss: SparkSession, hbaseModel: KeyValueModel): SparkStructuredStreamingWriter = {
    new HBaseStructuredStreamingWriter(hbaseModel, ss)
  }

  def createSparkStreamingWriter(keyValueBL: KeyValueBL, ssc: StreamingContext, hbaseModel: KeyValueModel): SparkLegacyStreamingWriter = {
    new HBaseStreamingWriter(hbaseModel, ssc)
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

class HBaseStreamingWriter(hbaseModel: KeyValueModel,
                           ssc: StreamingContext)
  extends SparkLegacyStreamingWriter with Logging {

  override def write(stream: DStream[String]): Unit = {
    // get sql context
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    // To avoid task not serializeble of spark
    val hbaseModelLocal = hbaseModel
    logger.info(s"Initialize DStream HBase writer: $hbaseModel")
    val dataFrameSchema = hbaseModelLocal.dataFrameSchema.get

    val configResources: String = hbaseModel.getOptionsMap().getOrElse(HBaseSparkConf.HBASE_CONFIG_LOCATION, "")
    val config = HBaseConfiguration.create()
    configResources.split(",").foreach(r => config.addResource(r))
    configResources.split(",").filter(r => (r != "") && new File(r).exists()).foreach(r => config.addResource(new Path(r)))

    val hBaseContext = new HBaseContext(ssc.sparkContext, config)
    val options: Map[String, String] = hbaseModel.getOptionsMap ++
      hbaseModel.avroSchemas.getOrElse(Map()) ++
      Seq(
        HBaseTableCatalog.tableCatalog -> hbaseModel.tableCatalog,
        KeyValueModel.metadataAvroSchemaKey -> KeyValueModel.metadataAvro,
        HBaseTableCatalog.newTable -> "4",
        "useAvroSchemaManager" -> hbaseModel.useAvroSchemaManager.toString
      )
    stream.foreachRDD {
      rdd =>
        if (!rdd.isEmpty()) {

          // create df from rdd using provided schema & spark's json datasource
          val schema: StructType = DataType.fromJson(dataFrameSchema).asInstanceOf[StructType]
          val data = sqlContext.read.schema(schema).json(rdd)
          // this is a very hacky way, but it is ok since we never use it... at least we shouldn't
          // and we should remove this
          val df = PutConverterFactory.convertAvroColumns(options, data)
          val putConverterFactory = PutConverterFactory(options, df)
          val convertToPut: InternalRow => Put = putConverterFactory.convertToPut
          hBaseContext
            .bulkPut(df.queryExecution.toRdd,
              putConverterFactory.getTableName(),
              convertToPut
            )
        }
    }
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