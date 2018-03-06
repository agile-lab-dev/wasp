package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.mongodb.scala.bson.BsonObjectId


/**
	* A model for a reader, composed by a name, an endpoint to read from, and a reader type defining the datastore to use.
	*
	* @param name the name of this reader model
	* @param endpointName the id of the endpoint to read from
	* @param readerType the type of the datastore to read from
	*/
case class ReaderModel(name: String, endpointName: String, readerType: ReaderType)

object ReaderModel {
	// helpers to create writer models for supported datastores
	def indexReader(name: String, indexName: String, product: String) = ReaderModel(name, indexName, ReaderType(Datastores.indexCategory, Option(product)))
	def elasticReader(name: String, indexName: String) = ReaderModel(name, indexName, ReaderType.elasticReaderType)
	def solrReader(name: String, indexName: String) = ReaderModel(name, indexName, ReaderType.solrReaderType)
	def keyValueReader(name: String, tableName: String, product: String) = ReaderModel(name, tableName, ReaderType(Datastores.keyValueCategory, Option(product)))
	def hbaseReader(name: String, tableName: String, product: String) = ReaderModel(name, tableName, ReaderType.hbaseReaderType)
	def topicReader(name: String, topicName: String, product: String) = ReaderModel(name, topicName, ReaderType(Datastores.topicCategory, Option(product)))
	def kafkaReader(name: String, topicName: String) = ReaderModel(name, topicName, ReaderType.kafkaReaderType)
	def rawReader(name: String, rawName: String) = ReaderModel(name, rawName, ReaderType.rawReaderType)
	def websocketReader(name: String, websocketName: String) = ReaderModel(name, websocketName, ReaderType.websocketReaderType)
	def jdbcReader(name: String, jdbcName: String) = ReaderModel(name, jdbcName, ReaderType.jdbcReaderType)
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
			case Datastores.databaseCategory => product.getOrElse(Datastores.jdbcProduct)
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
	val jdbcReaderType = ReaderType(Datastores.databaseCategory, Some(Datastores.jdbcProduct))
}