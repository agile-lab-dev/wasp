package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores.{DatastoreCategory, DatastoreProduct, StreamingSource}

case class StreamingReaderModel private[wasp] (name: String,
                                               datastoreModelName: String,
                                               datastoreProduct: DatastoreProduct,
                                               rateLimit: Option[Int],
                                               options: Map[String, String])

object StreamingReaderModel {
  import DatastoreProduct._

  def apply[DSC <: DatastoreCategory, DSP <: DatastoreProduct]
           (name: String,
            datastoreModel: DatastoreModel[DSC],
            datastoreProduct: DSP,
            rateLimit: Option[Int],
            options: Map[String, String] = Map.empty)
           (implicit ev: DSP <:< DSC, ev2: DSC <:< StreamingSource): StreamingReaderModel = {
    StreamingReaderModel(name, datastoreModel.name, datastoreProduct, rateLimit, options)
  }

  def topicReader(name: String,
                  topicModel: TopicModel,
                  rateLimit: Option[Int],
                  options: Map[String, String] = Map.empty) =
    apply(name, topicModel, GenericTopicProduct, rateLimit, options)
  def kafkaReader(name: String,
                  topicModel: TopicModel,
                  rateLimit: Option[Int],
                  options: Map[String, String] = Map.empty) =
    apply(name, topicModel, KafkaProduct, rateLimit, options)
  def kafkaReaderMultitopic(name: String,
                  multiTopicModel: MultiTopicModel,
                  rateLimit: Option[Int],
                  options: Map[String, String] = Map.empty) =
    apply(name, multiTopicModel, KafkaProduct, rateLimit, options)
  def websocketReader(name: String,
                      websocketModel: WebsocketModel,
                      rateLimit: Option[Int],
                      options: Map[String, String] = Map.empty) =
    apply(name, websocketModel, WebSocketProduct, rateLimit, options)
}
