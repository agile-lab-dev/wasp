package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.mongodb.scala.bson.BsonObjectId


/**
	* A model for a reader, composed by a name, an endpoint to read from, and a reader type defining the datastore to use.
	*
	* @param name the name of this reader model
	* @param endpointId the id of the endpoint to read from
	* @param readerType the type of the datastore to read from
	*/
case class ReaderModel(name: String, endpointId: BsonObjectId, readerType: ReaderType)

object ReaderModel {
	// helpers to create writer models for supported datastores
	def indexReader(name: String, indexId: BsonObjectId, product: String) = ReaderModel(name, indexId, ReaderType(Datastores.indexCategory, Option(product)))
	def elasticReader(name: String, indexId: BsonObjectId) = ReaderModel(name, indexId, ReaderType.elasticReaderType)
	def solrReader(name: String, indexId: BsonObjectId) = ReaderModel(name, indexId, ReaderType.solrReaderType)
	def keyValueReader(name: String, tableId: BsonObjectId, product: String) = ReaderModel(name, tableId, ReaderType(Datastores.keyValueCategory, Option(product)))
	def hbaseReader(name: String, tableId: BsonObjectId, product: String) = ReaderModel(name, tableId, ReaderType.hbaseReaderType)
	def topicReader(name: String, topicId: BsonObjectId, product: String) = ReaderModel(name, topicId, ReaderType(Datastores.topicCategory, Option(product)))
	def kafkaReader(name: String, topicId: BsonObjectId) = ReaderModel(name, topicId, ReaderType.kafkaReaderType)
	def rawReader(name: String, rawId: BsonObjectId) = ReaderModel(name, rawId, ReaderType.rawReaderType)
	def websocketReader(name: String, websocketId: BsonObjectId) = ReaderModel(name, websocketId, ReaderType.websocketReaderType)
}

/**
	* Encapsulates information related to the datastore: its `category` and the `product` that provides it.
	* If the `product` is not specified, the default one will be used.
	*
	* See the companion object for the supported categories and products.
	*
	* @param category the category of the datastore for this reader type
	* @param product the product that provides the datastore for this reader type
	*/
case class ReaderType(category: String, product: Option[String]) {
	/**
		* Returns the `product` that is provided for this reader type, using the configured default if not specified.
		*/
	def getActualProduct: String = {
		category match {
			case Datastores.indexCategory => product.getOrElse(ConfigManager.getWaspConfig.defaultIndexedDatastore)
			case Datastores.keyValueCategory => product.getOrElse(Datastores.hbaseProduct) // TODO support default product like with index category
			case Datastores.rawCategory => product.getOrElse(Datastores.rawProduct) // TODO support default product like with index category
			case Datastores.topicCategory => product.getOrElse(Datastores.kafkaProduct) // TODO support default product like with index category
			case Datastores.websocketCategory => product.getOrElse(Datastores.websocketProduct) // TODO support default product like with index category
			case unknownCategory => throw new IllegalArgumentException("Unknown reader category \"" + unknownCategory + "\" in reader type \"" + this + "\"")
		}
	}
}

object ReaderType {
	// ready-made ReaderTypes for supported products
	val elasticReaderType = ReaderType(Datastores.indexCategory, Some(Datastores.elasticProduct))
	val solrReaderType = ReaderType(Datastores.indexCategory, Some(Datastores.solrProduct))
	val hbaseReaderType = ReaderType(Datastores.keyValueCategory, Some(Datastores.hbaseProduct))
	val rawReaderType = ReaderType(Datastores.rawCategory, Some(Datastores.rawProduct))
	val kafkaReaderType = ReaderType(Datastores.topicCategory, Some(Datastores.kafkaProduct))
	val websocketReaderType = ReaderType(Datastores.websocketCategory, Some(Datastores.websocketProduct))
}