package it.agilelab.bigdata.wasp.consumers.spark.plugins

import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkLegacyStreamingWriter, SparkStructuredStreamingWriter, SparkWriter}
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
	* A WASP consumers Spark plugin provides streaming/batch read/write functionality for a particular `DatastoreProduct`
  * for Spark based consumers.
	*
	* @author Nicolò Bidotti
	*/
trait WaspConsumersSparkPlugin {
	def datastoreProduct: DatastoreProduct
	def initialize(waspDB: WaspDB)
	def getValidationRules: Seq[ValidationRule]
	def getSparkLegacyStreamingWriter(ssc: StreamingContext,
                                    legacyStreamingETLModel: LegacyStreamingETLModel,
                                    writerModel: WriterModel): SparkLegacyStreamingWriter
	def getSparkStructuredStreamingWriter(ss: SparkSession,
                                        structuredStreamingModel: StructuredStreamingETLModel,
                                        writerModel: WriterModel): SparkStructuredStreamingWriter
	def getSparkBatchWriter(sc: SparkContext,
                          writerModel: WriterModel): SparkWriter
	def getSparkBatchReader(sc: SparkContext,
                          readerModel: ReaderModel): SparkReader

}