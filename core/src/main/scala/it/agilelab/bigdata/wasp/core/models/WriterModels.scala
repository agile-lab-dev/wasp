package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores._


// TODO: switch to new apply as main ctor (see companion)
/**
	* A model for a writer, composed by a name, an datastoreModelName defining the datastore, and a datastoreProduct
	* defining the datastore software product to use.
	*
	* @param name the name of this writer model
	* @param datastoreModelName (optional) the name of the endpoint to write to
	* @param datastoreProduct the datastore software product to be used when writing
	*/
case class WriterModel @deprecated("Please use the other apply or the factory methods provided in the companion object as they ensure compatibility between the DatastoreModel and the DatastoreProduct")
                       (name: String, datastoreModelName: Option[String], datastoreProduct: DatastoreProduct) // why do we even normalize and not save the datastore model inside? it's bloody mongodb...

object WriterModel {
  // unfortunately we can't use this as the main ctor right now because DatastoreProduct doesn't have a working mongodb
  // codec and we need to write our own
	def apply[DSC <: DatastoreCategory, DSP <: DatastoreProduct]
           (name: String, datastoreModel: DatastoreModel[DSC], datastoreProduct: DSP)
           (implicit ev: DSP <:< DSC): WriterModel = {
		WriterModel(name, Some(datastoreModel.name), datastoreProduct)
	}
	// helpers to create writer models for supported datastores
	def indexWriter(name: String, indexModel: IndexModel) = apply(name, indexModel, GenericIndexProduct)
	def elasticWriter(name: String, indexModel: IndexModel) = apply(name, indexModel, ElasticProduct)
	def solrWriter(name: String, indexModel: IndexModel) = apply(name, indexModel, SolrProduct)
	def keyValueWriter(name: String, keyValueModel: KeyValueModel) = apply(name, keyValueModel, GenericKeyValueProduct)
	def hbaseWriter(name: String, keyValueModel: KeyValueModel) = apply(name, keyValueModel, HBaseProduct)
	def topicWriter(name: String, topicModel: TopicModel) = apply(name, topicModel, GenericTopicProduct)
	def kafkaWriter(name: String, topicModel: TopicModel) = apply(name, topicModel, KafkaProduct)
	def rawWriter(name: String, rawModel: RawModel) = apply(name, rawModel, RawProduct)
	def websocketWriter(name: String, websocketModel: WebsocketModel) = apply(name, websocketModel, WebSocketProduct)
	def consoleWriter(name: String) = apply(name, None, ConsoleProduct)
}