package it.agilelab.bigdata.wasp.consumers.spark.writers

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.streaming.dstream.DStream

trait SparkLegacyStreamingWriter {
  def write(stream: DStream[String])
}

trait SparkStructuredStreamingWriter {
  def write(stream: DataFrame): DataStreamWriter[Row]
}

trait SparkBatchWriter {
	def write(data: DataFrame) : Unit
}
