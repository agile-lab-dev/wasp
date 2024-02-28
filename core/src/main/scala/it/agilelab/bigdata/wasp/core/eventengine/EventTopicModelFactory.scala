package it.agilelab.bigdata.wasp.core.eventengine

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.KafkaProduct
import it.agilelab.bigdata.wasp.core.eventengine.settings.ModelSettings
import it.agilelab.bigdata.wasp.core.utils.JsonConverter
import it.agilelab.bigdata.wasp.models.{StreamingReaderModel, SubjectStrategy, TopicModel}

object EventTopicModelFactory {

  // TODO: move global variables to proper location
  val DEFAULT_PARTITIONS = "3"
  val DEFAULT_REPLICAS = "3"

  def create(modelSettings: ModelSettings): TopicModel = {
    lazy val eventTopicSchema: String = Event.SCHEMA.toString

    val topicName = modelSettings.dataStoreModelName
    val options = modelSettings.options

    TopicModel(
      name = TopicModel.name(topicName),
      creationTime = System.currentTimeMillis,
      partitions = options.getOrElse("partitions", DEFAULT_PARTITIONS).toInt,
      replicas = options.getOrElse("replicas", DEFAULT_REPLICAS).toInt,
      topicDataType = "avro",
      keyFieldName = options.get("keyFieldName"),
      headersFieldName = options.get("headersFieldName"),
      valueFieldsNames = options.get("valueFiledsName").flatMap(s => Some(s.split(","))), //Evaluate
      useAvroSchemaManager = true,
      schema = JsonConverter.fromString(eventTopicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument()),
      subjectStrategy = SubjectStrategy.Topic
    )
  }
}

object EventReaderModelFactory {

  /**
    * Bypass user's dedicated API in order to be able to generate StreamingReaderModel objects at runtime with configurable values
    *
    * @param modelSettings is the settings object containing parameters of the StreamingReaderModel
    * @return a new (KAFKA) StreamingReaderModel
    */
  def create(modelSettings: ModelSettings): StreamingReaderModel = {
    val name = modelSettings.modelName
    val dataStoreName = TopicModel.name(modelSettings.dataStoreModelName)
    val options = modelSettings.options
    val parsingMode = modelSettings.parsingMode
    val rateLimit: Option[Int] = options.get("rate-limit").flatMap(s => Some(s.toInt))

    new StreamingReaderModel(name, dataStoreName, KafkaProduct, rateLimit, options, parsingMode)
  }
}
