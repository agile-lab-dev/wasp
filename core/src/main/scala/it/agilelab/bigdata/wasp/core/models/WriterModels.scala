package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.mongodb.scala.bson.BsonObjectId


/**
	* A model for a writer, composed by a name, an endpoint to write to, and a writer type defining the datastore to use.
	*
	* @param name the name of this writer model
	* @param endpointId the id of the endpoint to write to
	* @param writerType the type of the datastore to write to
	*/
case class WriterModel(name: String, endpointId: BsonObjectId, writerType: WriterType)

object WriterModel {
	// helpers to create writer models for supported datastores
	def indexWriter(name: String, indexId: BsonObjectId, product: String) = WriterModel(name, indexId, WriterType(Datastores.indexCategory, Option(product)))
	def elasticWriter(name: String, indexId: BsonObjectId) = WriterModel(name, indexId, WriterType.elasticWriterType)
	def solrWriter(name: String, indexId: BsonObjectId) = WriterModel(name, indexId, WriterType.solrWriterType)
	def keyValueWriter(name: String, tableId: BsonObjectId, product: String) = WriterModel(name, tableId, WriterType(Datastores.keyValueCategory, Option(product)))
	def hbaseWriter(name: String, tableId: BsonObjectId, product: String) = WriterModel(name, tableId, WriterType.hbaseWriterType)
	def topicWriter(name: String, topicId: BsonObjectId, product: String) = WriterModel(name, topicId, WriterType(Datastores.topicCategory, Option(product)))
	def kafkaWriter(name: String, topicId: BsonObjectId) = WriterModel(name, topicId, WriterType.kafkaWriterType)
	def rawWriter(name: String, rawId: BsonObjectId) = WriterModel(name, rawId, WriterType.rawWriterType)
	def websocketWriter(name: String, websocketId: BsonObjectId) = WriterModel(name, websocketId, WriterType.websocketWriterType)
}

/**
	* Encapsulates information related to the datastore: its `category` and the `product` that provides it.
	* If the `product` is not specified, the default one will be used.
	*
	* See the companion object for the supported categories and products.
	*
	* @param category the category of the datastore for this writer type
	* @param product the product that provides the datastore for this writer type
	*/
case class WriterType(category: String, product: Option[String]) {
	/**
		* Returns the `product` that is provided for this writer type, using the configured default if not specified.
		*/
	def getActualProduct: String = {
		category match {
			case Datastores.indexCategory => product.getOrElse(ConfigManager.getWaspConfig.defaultIndexedDatastore)
			case Datastores.keyValueCategory => product.getOrElse(Datastores.hbaseProduct) // TODO support default product like with index category
			case Datastores.rawCategory => product.getOrElse(Datastores.rawProduct) // TODO support default product like with index category
			case Datastores.topicCategory => product.getOrElse(Datastores.kafkaProduct) // TODO support default product like with index category
			case Datastores.websocketCategory => product.getOrElse(Datastores.websocketProduct) // TODO support default product like with index category
			case unknownCategory => throw new IllegalArgumentException("Unknown writer category \"" + unknownCategory + "\" in writer type \"" + this + "\"")
		}
	}
}

object WriterType {
	// ready-made WriterTypes for supported products
	val elasticWriterType = WriterType(Datastores.indexCategory, Some(Datastores.elasticProduct))
	val solrWriterType = WriterType(Datastores.indexCategory, Some(Datastores.solrProduct))
	val hbaseWriterType = WriterType(Datastores.keyValueCategory, Some(Datastores.hbaseProduct))
	val rawWriterType = WriterType(Datastores.rawCategory, Some(Datastores.rawProduct))
	val kafkaWriterType = WriterType(Datastores.topicCategory, Some(Datastores.kafkaProduct))
	val websocketWriterType = WriterType(Datastores.websocketCategory, Some(Datastores.websocketProduct))
}
