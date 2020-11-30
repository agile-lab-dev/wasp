package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.{DatastoreCategory, DatastoreProduct}


/**
	* A model for a writer, composed by a name, a datastoreModelName defining the datastore, a datastoreProduct
	* defining the datastore software product to use, and any additional options needed to configure the writer.
	*
	* @param name the name of this writer model
	* @param datastoreModelName the name of the endpoint to write to; ignored when using a `ConsoleCategory` datastore
	* @param datastoreProduct the datastore software product to be used when writing
  * @param options additional options for the writer
	*/
case class WriterModel @deprecated(WriterModel.deprecationMessage) private[wasp]
                      (name: String,
                       datastoreModelName: String,
                       datastoreProduct: DatastoreProduct,
                       options: Map[String, String])

object WriterModel {
  import DatastoreProduct._
	
	def apply[DSC <: DatastoreCategory, DSP <: DatastoreProduct]
           (name: String,
            datastoreModel: DatastoreModel[DSC],
            datastoreProduct: DSP,
            options: Map[String, String] = Map.empty)
           (implicit ev: DSP <:< DSC): WriterModel = {
		WriterModel(name, datastoreModel.name, datastoreProduct, options)
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
	def kafkaMultitopicWriter(name: String, multiTopicModel: MultiTopicModel, options: Map[String, String] = Map.empty) =
		apply(name, multiTopicModel, KafkaProduct, options)
	def rawWriter(name: String, rawModel: RawModel, options: Map[String, String] = Map.empty) =
    apply(name, rawModel, RawProduct, options)
	def websocketWriter(name: String, websocketModel: WebsocketModel, options: Map[String, String] = Map.empty) =
    apply(name, websocketModel, WebSocketProduct, options)
	def webMailWriter(name: String, webMailModel: WebMailModel, options: Map[String, String] = Map.empty) =
		apply(name, webMailModel, WebMailProduct, options)
	def consoleWriter(name: String, options: Map[String, String] = Map.empty) =
    apply(name, ConsoleProduct.getActualProductName, ConsoleProduct, options)
	def mongoDbWriter(name: String, documentModel: DocumentModel, options: Map[String, String]): WriterModel =
		apply(name, documentModel, MongoDbProduct, options)
	def httpWriter(name: String, httpModel: HttpModel, options: Map[String, String] = Map.empty): WriterModel =
		apply(name, httpModel, HttpProduct, options)

  // this exists because we want to keep the main declaration on one line because of a quirk of the compiler when
  // using both an annotation and an access modifier: it doesn't allow us to break it into more than one line,
  // complaining about the case class missing a parameter list
  private val deprecationMessage = "Please use the other apply or the factory methods provided in the companion " +
      "object as they ensure compatibility between the DatastoreModel and the DatastoreProduct"
}