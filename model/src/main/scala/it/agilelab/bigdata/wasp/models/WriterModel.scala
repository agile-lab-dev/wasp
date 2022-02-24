package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}

/**
	* A model for a writer, composed by a name, a datastoreModelName defining the datastore, a datastoreProduct
	* defining the datastore software product to use, and any additional options needed to configure the writer.
	*
	* @param name the name of this writer model
	* @param datastoreModelName the name of the endpoint to write to; ignored when using a `ConsoleCategory` datastore
	* @param datastoreProduct the datastore software product to be used when writing
  * @param options additional options for the writer
	*/
case class WriterModel private[wasp](
    name: String,
    datastoreModelName: String,
    datastoreProduct: DatastoreProduct,
    options: Map[String, String]
)

object WriterModel {
  import DatastoreProduct._

  def apply(
      name: String,
      datastoreModel: DatastoreModel,
      datastoreProduct: DatastoreProduct,
      options: Map[String, String] = Map.empty
  ): WriterModel = {
    WriterModel(name, datastoreModel.name, datastoreProduct, options)
  }

  def indexWriter(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty) =
    apply(name, indexModel, GenericIndexProduct, options)
  def elasticWriter(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty) =
    apply(name, indexModel.name, ElasticProduct, options)
  def solrWriter(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty) =
    apply(name, indexModel.name, SolrProduct, options)
  def keyValueWriter(name: String, keyValueModel: KeyValueModel, options: Map[String, String] = Map.empty) =
    apply(name, keyValueModel.name, GenericKeyValueProduct, options)
  def hbaseWriter(name: String, keyValueModel: KeyValueModel, options: Map[String, String] = Map.empty) =
    apply(name, keyValueModel.name, HBaseProduct, options)
  def topicWriter(name: String, topicModel: TopicModel, options: Map[String, String] = Map.empty) =
    apply(name, topicModel.name, GenericTopicProduct, options)
  def kafkaWriter(name: String, topicModel: TopicModel, options: Map[String, String] = Map.empty) =
    apply(name, topicModel.name, KafkaProduct, options)
  def kafkaMultitopicWriter(name: String, multiTopicModel: MultiTopicModel, options: Map[String, String] = Map.empty) =
    apply(name, multiTopicModel.name, KafkaProduct, options)
  def rawWriter(name: String, rawModel: RawModel, options: Map[String, String] = Map.empty) =
    apply(name, rawModel.name, RawProduct, options)
  def websocketWriter(name: String, websocketModel: WebsocketModel, options: Map[String, String] = Map.empty) =
    apply(name, websocketModel.name, WebSocketProduct, options)
  def webMailWriter(name: String, webMailModel: WebMailModel, options: Map[String, String] = Map.empty) =
    apply(name, webMailModel.name, WebMailProduct, options)
  def consoleWriter(name: String, options: Map[String, String] = Map.empty) =
    apply(name, ConsoleProduct.getActualProductName, ConsoleProduct, options)
  def mongoDbWriter(name: String, documentModel: DocumentModel, options: Map[String, String]) =
    apply(name, documentModel.name, MongoDbProduct, options)
  def httpWriter(name: String, httpModel: HttpModel, options: Map[String, String] = Map.empty): WriterModel =
    apply(name, httpModel, HttpProduct, options)
  def cdcWriter(name: String, model: CdcModel, options: Map[String, String] = Map.empty) =
    apply(name, model, CdcProduct, options)

  def genericWriter(name: String, genericModel: GenericModel, options: Map[String, String] = Map.empty): WriterModel =
    apply(name, genericModel, genericModel.product, options)
}
