package it.agilelab.bigdata.wasp.consumers.spark.plugins

import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkStructuredStreamingWriter}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.repository.core.db.WaspDB
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.models.{ReaderModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


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

	def getSparkStructuredStreamingWriter(ss: SparkSession,
                                        structuredStreamingModel: StructuredStreamingETLModel,
                                        writerModel: WriterModel): SparkStructuredStreamingWriter
	def getSparkStructuredStreamingReader(ss: SparkSession,
	                                      structuredStreamingETLModel: StructuredStreamingETLModel,
	                                      streamingReaderModel: StreamingReaderModel): SparkStructuredStreamingReader
	def getSparkBatchWriter(sc: SparkContext,
                          writerModel: WriterModel): SparkBatchWriter
	def getSparkBatchReader(sc: SparkContext,
                          readerModel: ReaderModel): SparkBatchReader
}