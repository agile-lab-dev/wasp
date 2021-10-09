package it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc

import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.CdcModel
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class CdcSparkStructuredStreamingWriter(writer: Writer, model: CdcModel, ss: SparkSession) extends SparkStructuredStreamingWriter with Logging {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {

    val path = model.uri
    val options = model.options
    val mode = if (options.saveMode == "default") "append" else model.options.saveMode
    val format = options.format
    val extraOptions = options.extraOptions.getOrElse(Map())

    stream.writeStream
      .format(format)
      .outputMode(mode)
      .options(extraOptions)
      .option("path", path)
      .foreachBatch((df: DataFrame, id: Long) =>
        writer.write(df, id)
      )
  }
}
