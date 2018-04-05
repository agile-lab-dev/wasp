package it.agilelab.bigdata.wasp.consumers.spark.plugins

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.models.WriterModel
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
	* Interface for a WASP consumer spark plugin
	*
	* @author Nicolò Bidotti
	*/
trait WaspConsumersSparkPlugin {
	def initialize(waspDB: WaspDB)
	def getSparkLegacyStreamingWriter(ssc: StreamingContext, writerModel: WriterModel): SparkLegacyStreamingWriter
	def getSparkStructuredStreamingWriter(ss: SparkSession, writerModel: WriterModel): SparkStructuredStreamingWriter
	def getSparkWriter(sc: SparkContext, writerModel: WriterModel): SparkWriter
	def getSparkReader(id: String, name: String): SparkReader
	def pluginType: String
	def getValidationRules(): Seq[ValidationRule]
}