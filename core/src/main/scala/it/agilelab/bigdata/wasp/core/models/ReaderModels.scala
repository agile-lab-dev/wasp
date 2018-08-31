package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores._


/**
	* A model for a reader, composed by a name, an datastoreModelName defining the datastore, and a datastoreProduct
	* defining the datastore software product to use.
	*
	* @param name the name of this reader model
	* @param datastoreModelName (optional) the name of the endpoint to read from
	* @param datastoreProduct the datastore software product to be used when reading
	*/
case class ReaderModel @deprecated("Please use the other apply or the factory methods provided in the companion object as they ensure compatibility between the DatastoreModel and the DatastoreProduct")
                       (name: String, datastoreModelName: String, datastoreProduct: DatastoreProduct) // why do we even normalize and not save the datastore model inside? it's bloody mongodb...

object ReaderModel {
	def apply[DSC <: DatastoreCategory, DSP <: DatastoreProduct](name: String, datastoreModel: DatastoreModel[DSC], datastoreProduct: DSP)(implicit ev: DSP <:< DSC) = {
		ReaderModel(name, datastoreModel.name, datastoreProduct)
	}
	def indexReader(name: String, indexModel: IndexModel) = apply(name, indexModel, GenericIndexProduct)
	def elasticReader(name: String, indexModel: IndexModel) = apply(name, indexModel, ElasticProduct)
	def solrReader(name: String, indexModel: IndexModel) = apply(name, indexModel, SolrProduct)
	def keyValueReader(name: String, keyValueModel: KeyValueModel) = apply(name, keyValueModel, GenericKeyValueProduct)
	def hbaseReader(name: String, keyValueModel: KeyValueModel) = apply(name, keyValueModel, HBaseProduct)
	def topicReader(name: String, topicModel: TopicModel) = apply(name, topicModel, GenericTopicProduct)
	def kafkaReader(name: String, topicModel: TopicModel) = apply(name, topicModel, KafkaProduct)
	def rawReader(name: String, rawModel: RawModel) = apply(name, rawModel, RawProduct)
	def websocketReader(name: String, websocketModel: WebsocketModel) = apply(name, websocketModel, WebSocketProduct)
}