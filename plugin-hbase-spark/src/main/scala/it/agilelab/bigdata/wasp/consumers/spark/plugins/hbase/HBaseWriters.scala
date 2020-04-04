package it.agilelab.bigdata.wasp.consumers.spark.plugins.hbase

import java.io.File

import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.KeyValueModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.spark.{HBaseContext, PutConverterFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object HBaseWriter {
  def createSparkStructuredStreamingWriter(keyValueBL: KeyValueBL, ss: SparkSession, hbaseModel: KeyValueModel): SparkStructuredStreamingWriter = {
    new HBaseStructuredStreamingWriter(hbaseModel, ss)
  }

  def createSparkStreamingWriter(keyValueBL: KeyValueBL, ssc: StreamingContext, hbaseModel: KeyValueModel): SparkLegacyStreamingWriter = {
    new HBaseStreamingWriter(hbaseModel, ssc)
  }

  def createSparkWriter(keyValueBL: KeyValueBL, sc: SparkContext, hbaseModel: KeyValueModel): SparkWriter = {
    new HBaseWriter(hbaseModel, sc)
  }
}

class HBaseStructuredStreamingWriter(hbaseModel: KeyValueModel,
                                     ss: SparkSession)
  extends SparkStructuredStreamingWriter {
  override def write(stream: DataFrame, queryName: String, checkpointDir: String): StreamingQuery = {
    val options: Map[String, String] = hbaseModel.getOptionsMap ++
    hbaseModel.avroSchemas.getOrElse(Map()) ++
    Seq(
      HBaseTableCatalog.tableCatalog -> hbaseModel.tableCatalog,
      KeyValueModel.metadataAvroSchemaKey -> KeyValueModel.metadataAvro,
      HBaseTableCatalog.newTable -> "4"
    )
    val streamWriter = stream.writeStream
      .options(options)
      .option("checkpointLocation", checkpointDir)
      .queryName(queryName)
      .format("org.apache.hadoop.hbase.spark")

    if(ConfigManager.getSparkStreamingConfig.triggerIntervalMs.isDefined)
      streamWriter
        .trigger(Trigger.ProcessingTime(ConfigManager.getSparkStreamingConfig.triggerIntervalMs.get))
        .start()
    else
      streamWriter.start()

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
        HBaseTableCatalog.newTable -> "4"
      )
    stream.foreachRDD {
      rdd =>
        if (!rdd.isEmpty()) {

          // create df from rdd using provided schema & spark's json datasource
          val schema: StructType = DataType.fromJson(dataFrameSchema).asInstanceOf[StructType]
          val df = sqlContext.read.json(rdd)

          val putConverterFactory = PutConverterFactory(options, schema)
          val convertToPut: Row => Put = putConverterFactory.convertToPut
          hBaseContext
            .bulkPut(df.rdd,
              putConverterFactory.getTableName(),
              convertToPut
            )
        }
    }
  }
}

class HBaseWriter(hbaseModel: KeyValueModel,
                  sc: SparkContext)
  extends SparkWriter {

  override def write(df: DataFrame): Unit = {
    val options: Map[String, String] = hbaseModel.getOptionsMap ++
    hbaseModel.avroSchemas.getOrElse(Map()) ++
    Seq(
      HBaseTableCatalog.tableCatalog -> hbaseModel.tableCatalog,
      KeyValueModel.metadataAvroSchemaKey -> KeyValueModel.metadataAvro,
      HBaseTableCatalog.newTable -> "4"
    )

    df.write
      .options(options)
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
}