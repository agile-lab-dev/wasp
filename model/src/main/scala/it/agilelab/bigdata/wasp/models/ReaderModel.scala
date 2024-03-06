package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores._

/**
	* A model for a reader, composed by a name, a datastoreModelName defining the datastore, a datastoreProduct
	* defining the datastore software product to use, and any additional options needed to configure the reader.
	*
	* @param name the name of this reader model
	* @param datastoreModelName (optional) the name of the endpoint to read from
	* @param datastoreProduct the datastore software product to be used when reading
  * @param options additional options for the reader
	*/
case class ReaderModel private[wasp] (
    name: String,
    datastoreModelName: String,
    datastoreProduct: DatastoreProduct,
    options: Map[String, String]
)

object ReaderModel {
  import DatastoreProduct._

  def apply(
      name: String,
      datastoreModel: DatastoreModel,
      datastoreProduct: DatastoreProduct,
      options: Map[String, String] = Map.empty
  ): ReaderModel = {
    new ReaderModel(name, datastoreModel.name, datastoreProduct, options)
  }

  def jdbcReader(
      name: String,
      sqlSourceModel: SqlSourceModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, sqlSourceModel, JDBCProduct, options)
  def indexReader(
      name: String,
      indexModel: IndexModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, indexModel, GenericIndexProduct, options)
  def elasticReader(
      name: String,
      indexModel: IndexModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, indexModel, ElasticProduct, options)
  def solrReader(
      name: String,
      indexModel: IndexModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, indexModel, SolrProduct, options)
  def keyValueReader(
      name: String,
      keyValueModel: KeyValueModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, keyValueModel, GenericKeyValueProduct, options)
  def hbaseReader(
      name: String,
      keyValueModel: KeyValueModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, keyValueModel, HBaseProduct, options)
  def topicReader(
      name: String,
      topicModel: TopicModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, topicModel, GenericTopicProduct, options)
  def kafkaReader(
      name: String,
      topicModel: TopicModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, topicModel, KafkaProduct, options)
  def kafkaReaderMultitopic(
      name: String,
      multiTopicModel: MultiTopicModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, multiTopicModel, KafkaProduct, options)
  def rawReader(
      name: String,
      rawModel: RawModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, rawModel, RawProduct, options)
  def websocketReader(
      name: String,
      websocketModel: WebsocketModel,
      options: Map[String, String] = Map.empty
  ) =
    apply(name, websocketModel, WebSocketProduct, options)
  def mongoDbReader(
      name: String,
      documentModel: DocumentModel,
      options: Map[String, String]
  ): ReaderModel =
    apply(name, documentModel, MongoDbProduct, options)

}
