package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.models.configuration.{ParsingMode, Strict}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

case class StreamingReaderModel private[wasp] (
    name: String,
    datastoreModelName: String,
    datastoreProduct: DatastoreProduct,
    rateLimit: Option[Int],
    options: Map[String, String],
    parsingMode: ParsingMode
)

object StreamingReaderModel {
  import DatastoreProduct._

  def apply(
      name: String,
      datastoreModel: DatastoreModel,
      datastoreProduct: DatastoreProduct,
      rateLimit: Option[Int],
      options: Map[String, String] = Map.empty,
      parsingMode: ParsingMode = Strict
  ): StreamingReaderModel = {
    StreamingReaderModel(name, datastoreModel.name, datastoreProduct, rateLimit, options, parsingMode)
  }

  def topicReader(
      name: String,
      topicModel: TopicModel,
      rateLimit: Option[Int],
      options: Map[String, String] = Map.empty,
      parsingMode: ParsingMode = Strict

  ): StreamingReaderModel =
    apply(name, topicModel.name, GenericTopicProduct, rateLimit, options, parsingMode)

  def kafkaReader(
      name: String,
      topicModel: TopicModel,
      rateLimit: Option[Int],
      options: Map[String, String] = Map.empty,
      parsingMode: ParsingMode = Strict
  ): StreamingReaderModel =
    apply(name, topicModel.name, KafkaProduct, rateLimit, options, parsingMode)

  def kafkaReaderMultitopic(
      name: String,
      multiTopicModel: MultiTopicModel,
      rateLimit: Option[Int],
      options: Map[String, String] = Map.empty,
      parsingMode: ParsingMode = Strict
  ): StreamingReaderModel =
    apply(name, multiTopicModel.name, KafkaProduct, rateLimit, options, parsingMode)

  def rawReader(name: String, rawModel: RawModel, options: Map[String, String] = Map.empty,
                parsingMode: ParsingMode = Strict): StreamingReaderModel =
    apply(name, rawModel.name, RawProduct, None, options, parsingMode)

  def websocketReader(
      name: String,
      websocketModel: WebsocketModel,
      rateLimit: Option[Int],
      options: Map[String, String] = Map.empty,
      parsingMode: ParsingMode = Strict
  ): StreamingReaderModel =
    apply(name, websocketModel.name, WebSocketProduct, rateLimit, options, parsingMode)
}
