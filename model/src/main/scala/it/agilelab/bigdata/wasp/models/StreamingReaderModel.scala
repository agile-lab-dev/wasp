package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}

case class StreamingReaderModel private[wasp] (name: String,
                                               datastoreModelName: String,
                                               datastoreProduct: DatastoreProduct,
                                               rateLimit: Option[Int],
                                               options: Map[String, String])

object StreamingReaderModel {
  import DatastoreProduct._

  def apply(name: String,
            datastoreModel: DatastoreModel,
            datastoreProduct: DatastoreProduct,
            rateLimit: Option[Int],
            options: Map[String, String] = Map.empty): StreamingReaderModel = {
    StreamingReaderModel(name, datastoreModel.name, datastoreProduct, rateLimit, options)
  }

  def topicReader(name: String,
                  topicModel: TopicModel,
                  rateLimit: Option[Int],
                  options: Map[String, String] = Map.empty) =
    apply(name, topicModel.name, GenericTopicProduct, rateLimit, options)
  def kafkaReader(name: String,
                  topicModel: TopicModel,
                  rateLimit: Option[Int],
                  options: Map[String, String] = Map.empty) =
    apply(name, topicModel.name, KafkaProduct, rateLimit, options)
  def kafkaReaderMultitopic(name: String,
                  multiTopicModel: MultiTopicModel,
                  rateLimit: Option[Int],
                  options: Map[String, String] = Map.empty) =
    apply(name, multiTopicModel.name, KafkaProduct, rateLimit, options)
  def websocketReader(name: String,
                      websocketModel: WebsocketModel,
                      rateLimit: Option[Int],
                      options: Map[String, String] = Map.empty) =
    apply(name, websocketModel.name, WebSocketProduct, rateLimit, options)
}
