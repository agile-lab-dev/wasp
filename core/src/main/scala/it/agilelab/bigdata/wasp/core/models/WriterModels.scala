package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores._


// TODO: switch to new apply as main ctor (see companion)
/**
	* A model for a writer, composed by a name, a datastoreModelName defining the datastore, a datastoreProduct
	* defining the datastore software product to use, and any additional options needed to configure the writer.
	*
	* @param name the name of this writer model
	* @param datastoreModelName (optional) the name of the endpoint to write to
	* @param datastoreProduct the datastore software product to be used when writing
  * @param options additional options for the writer
	*/
case class WriterModel @deprecated("Please use the other apply or the factory methods provided in the companion " +
                                   "object as they ensure compatibility between the DatastoreModel and the " +
                                   "DatastoreProduct")
                       (name: String, datastoreModelName: Option[String], datastoreProduct: DatastoreProduct, options: Map[String, String] = Map.empty)

object WriterModel {
  // unfortunately we can't use this as the main ctor right now because DatastoreProduct doesn't have a working mongodb
  // codec and we need to write our own
	def apply[DSC <: DatastoreCategory, DSP <: DatastoreProduct]
           (name: String, datastoreModel: DatastoreModel[DSC], datastoreProduct: DSP, options: Map[String, String] = Map.empty)
           (implicit ev: DSP <:< DSC): WriterModel = {
		WriterModel(name, Some(datastoreModel.name), datastoreProduct, options)
	}
	def indexWriter(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty) =
    apply(name, indexModel, GenericIndexProduct, options)
	def elasticWriter(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty) =
    apply(name, indexModel, ElasticProduct, options)
	def solrWriter(name: String, indexModel: IndexModel, options: Map[String, String] = Map.empty) =
    apply(name, indexModel, SolrProduct, options)
	def keyValueWriter(name: String, keyValueModel: KeyValueModel, options: Map[String, String] = Map.empty) =
    apply(name, keyValueModel, GenericKeyValueProduct, options)
	def hbaseWriter(name: String, keyValueModel: KeyValueModel, options: Map[String, String] = Map.empty) =
    apply(name, keyValueModel, HBaseProduct, options)
	def topicWriter(name: String, topicModel: TopicModel, options: Map[String, String] = Map.empty) =
    apply(name, topicModel, GenericTopicProduct, options)
	def kafkaWriter(name: String, topicModel: TopicModel, options: Map[String, String] = Map.empty) =
    apply(name, topicModel, KafkaProduct, options)
	def rawWriter(name: String, rawModel: RawModel, options: Map[String, String] = Map.empty) =
    apply(name, rawModel, RawProduct, options)
	def websocketWriter(name: String, websocketModel: WebsocketModel, options: Map[String, String] = Map.empty) =
    apply(name, websocketModel, WebSocketProduct, options)
	def consoleWriter(name: String, options: Map[String, String] = Map.empty) =
    apply(name, None, ConsoleProduct, options)
}