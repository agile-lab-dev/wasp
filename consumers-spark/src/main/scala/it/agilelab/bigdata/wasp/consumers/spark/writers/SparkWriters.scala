package it.agilelab.bigdata.wasp.consumers.spark.writers

import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.DStream

trait SparkStreamingWriter {
  def write(stream: DStream[String])
}

trait SparkStructuredStreamingWriter {
  def write(stream: DataFrame)
}

trait SparkWriter {
	def write(data: DataFrame)
}

