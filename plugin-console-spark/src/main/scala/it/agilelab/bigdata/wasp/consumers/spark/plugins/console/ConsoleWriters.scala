package it.agilelab.bigdata.wasp.consumers.spark.plugins.console

import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStructuredStreamingWriter, SparkBatchWriter}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

class ConsoleSparkStructuredStreamingWriter()
  extends SparkStructuredStreamingWriter {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    // configure and start streaming
    stream.writeStream
      .format("console")
      .outputMode("append")
  }
}

class ConsoleSparkBatchWriter extends SparkBatchWriter {

  override def write(data: DataFrame): Unit = ConsoleWriters.write(data)

}

object ConsoleWriters {

  def write(dataframe: DataFrame): Unit = dataframe.show() // truncate

}