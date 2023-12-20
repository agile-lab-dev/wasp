package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores._
import it.agilelab.bigdata.wasp.models.configuration.{ParsingMode, Strict}

/**
	* A model for a reader, composed by a name, a datastoreModelName defining the datastore, a datastoreProduct
	* defining the datastore software product to use, and any additional options needed to configure the reader.
	*
	* @param name the name of this reader model
	* @param datastoreModelName (optional) the name of the endpoint to read from
	* @param datastoreProduct the datastore software product to be used when reading
  * @param options additional options for the reader
	*/

case class ReaderModel private[wasp](
    name: String,
    datastoreModelName: String,
    datastoreProduct: DatastoreProduct,
    options: Map[String, String],
    parsingMode: ParsingMode
)

object ReaderModel {
  import DatastoreProduct._

  def apply(
      name: String,
      datastoreModel: DatastoreModel,
      datastoreProduct: DatastoreProduct,
      options: Map[String, String] = Map.empty,
      parsingMode: ParsingMode = Strict
  ): ReaderModel = {
    ReaderModel(name, datastoreModel, datastoreProduct, options, parsingMode)
  }

	def jdbcReader(name: String, sqlSourceModel: SqlSourceModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
		apply(name, sqlSourceModel, JDBCProduct, options, parsingMode)
	def indexReader(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, indexModel, GenericIndexProduct, options, parsingMode)
	def elasticReader(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, indexModel, ElasticProduct, options, parsingMode)
	def solrReader(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, indexModel, SolrProduct, options, parsingMode)
	def keyValueReader(name: String, keyValueModel: KeyValueModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, keyValueModel, GenericKeyValueProduct, options, parsingMode)
	def hbaseReader(name: String, keyValueModel: KeyValueModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, keyValueModel, HBaseProduct, options, parsingMode)
	def topicReader(name: String, topicModel: TopicModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, topicModel, GenericTopicProduct, options, parsingMode)
	def kafkaReader(name: String, topicModel: TopicModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, topicModel, KafkaProduct, options, parsingMode)
	def kafkaReaderMultitopic(name: String, multiTopicModel: MultiTopicModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
		apply(name, multiTopicModel, KafkaProduct, options, parsingMode)
	def rawReader(name: String, rawModel: RawModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, rawModel, RawProduct, options, parsingMode)
	def websocketReader(name: String, websocketModel: WebsocketModel, options: Map[String, String] = Map.empty, parsingMode: ParsingMode = Strict) =
    apply(name, websocketModel, WebSocketProduct, options, parsingMode)
	def mongoDbReader(name: String, documentModel: DocumentModel, options: Map[String, String], parsingMode: ParsingMode = Strict): ReaderModel =
		apply(name, documentModel, MongoDbProduct, options, parsingMode)

}
