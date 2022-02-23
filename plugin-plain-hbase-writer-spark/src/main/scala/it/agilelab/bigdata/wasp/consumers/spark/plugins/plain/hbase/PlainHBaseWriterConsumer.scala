package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase

import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.core.exceptions.ModelNotFound
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.HBaseProduct
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, KeyValueBL}
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class PlainHBaseWriterConsumer extends WaspConsumersSparkPlugin with Logging {

  var keyValueBL: KeyValueBL = _

  override def datastoreProduct: DatastoreProduct = HBaseProduct

  override def initialize(waspDB: WaspDB): Unit = {
    logger.info("Initialize the keyValue BL")
    keyValueBL = ConfigBL.keyValueBL
  }

  override def getSparkStructuredStreamingWriter(ss: SparkSession,
                                                 structuredStreamingModel: StructuredStreamingETLModel,
                                                 writerModel: WriterModel): SparkStructuredStreamingWriter = {
    new HBaseStructuredStreamingWriter(getKeyValueModel(writerModel))
  }

  override def getValidationRules: Seq[ValidationRule] = Seq.empty

  override def getSparkStructuredStreamingReader(ss: SparkSession,
                                                 structuredStreamingETLModel: StructuredStreamingETLModel,
                                                 streamingReaderModel: StreamingReaderModel)
  : SparkStructuredStreamingReader = {
    throw new NotImplementedError("This plugin does not support read operation of any kind!")
  }

  override def getSparkBatchWriter(sc: SparkContext,
                                   writerModel: WriterModel): SparkBatchWriter = {
    throw new NotImplementedError("This plugin does not support spark batch")
  }

  override def getSparkBatchReader(sc: SparkContext,
                                   readerModel: ReaderModel): SparkBatchReader = {
    throw new NotImplementedError("This plugin does not support read operation of any kind!")
  }

  @throws(classOf[ModelNotFound])
  private def getKeyValueModel(writerModel: WriterModel): KeyValueModel = {

    val endpointName = writerModel.datastoreModelName
    val hbaseModelOpt = keyValueBL.getByName(endpointName)
    if (hbaseModelOpt.isDefined) {
      hbaseModelOpt.get
    } else {
      throw new ModelNotFound(s"The KeyValueModel with this name $endpointName not found")
    }
  }
}

class HBaseStructuredStreamingWriter(hbaseModel: KeyValueModel) extends SparkStructuredStreamingWriter with Logging {
  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val options = hbaseModel.getOptionsMap ++ Seq("newTable" -> "4")

    logger.info(s"HBase writer options: ${options.mkString(";")}")

    stream
      .writeStream
      .options(options)
      .format("it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration")
  }
}
