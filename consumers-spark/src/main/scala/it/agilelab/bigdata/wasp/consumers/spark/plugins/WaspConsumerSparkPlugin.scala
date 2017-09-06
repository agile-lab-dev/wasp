package it.agilelab.bigdata.wasp.consumers.spark.plugins

import it.agilelab.bigdata.wasp.consumers.spark.readers.StaticReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.models.WriterModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

/**
	* Interface for a WASP consumer spark plugin
	*
	* @author Nicolò Bidotti
	*/
trait WaspConsumerSparkPlugin {
	def initialize(waspDB: WaspDB)
	def  getSparkStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkStreamingWriter
	def  getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter
	def  getSparkReader(id: String, name: String): StaticReader
	def  pluginType: String
}