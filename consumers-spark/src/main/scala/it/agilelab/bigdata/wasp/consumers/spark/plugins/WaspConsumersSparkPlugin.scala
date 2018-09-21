package it.agilelab.bigdata.wasp.consumers.spark.plugins

import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkLegacyStreamingWriter, SparkStructuredStreamingWriter}
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
	def getSparkLegacyStreamingReader(ssc: StreamingContext,
	                                  legacyStreamingETLModel: LegacyStreamingETLModel,
	                                  readerModel: ReaderModel): SparkLegacyStreamingReader
	def getSparkStructuredStreamingWriter(ss: SparkSession,
                                        structuredStreamingModel: StructuredStreamingETLModel,
                                        writerModel: WriterModel): SparkStructuredStreamingWriter
	def getSparkStructuredStreamingReader(ss: SparkSession,
	                                      structuredStreamingETLModel: StructuredStreamingETLModel,
	                                      readerModel: ReaderModel): SparkStructuredStreamingReader
	def getSparkBatchWriter(sc: SparkContext,
                          writerModel: WriterModel): SparkBatchWriter
	def getSparkBatchReader(sc: SparkContext,
                          readerModel: ReaderModel): SparkBatchReader
}