package it.agilelab.bigdata.wasp.models.editor

import it.agilelab.bigdata.wasp.models.RawModel
import spray.json.JsObject

/**
  * Pipegraph data transfer object
  * @param name name of the pipegraph
  * @param description description of the pipegraph
  * @param owner owner of the pipegraph
  * @param structuredStreamingComponents components describing processing built on Spark Structured Streaming
  *
  */
case class PipegraphDTO(
    name: String,
    description: String,
    owner: Option[String],
    structuredStreamingComponents: List[StructuredStreamingETLDTO]
)

/**
  * StructuredStreamingETLModel data transfer object
  * @param name unique name of the processing component
  * @param group group of which the processing component is part
  * @param streamingInput streaming input unique name
  * @param streamingOutput streaming output definition
  * @param strategy strategy model that defines the processing
  * @param triggerIntervalMs trigger interval to use, in milliseconds
  */
case class StructuredStreamingETLDTO(
    name: String,
    group: String,
    streamingInput: ReaderModelDTO,
    streamingOutput: WriterModelDTO,
    strategy: StrategyDTO,
    triggerIntervalMs: Option[Long],
    options: Map[String, String]
)

/**
  * Datastore model DTO case classes
  */
sealed trait DatastoreModelDTO

object DatastoreModelDTO {
  val topicType    = "topic"
  val indexType    = "index"
  val keyValueType = "keyvalue"
  val rawDataType  = "rawdata"
}

case class TopicModelDTO(name: String) extends DatastoreModelDTO
case class IndexModelDTO(name: String) extends DatastoreModelDTO
case class KeyValueModelDTO(name: String) extends DatastoreModelDTO
case class RawModelDTO(name: String, config: Option[RawModel]) extends DatastoreModelDTO

case class RawModelSetupDTO(
    uri: String,
    timed: Boolean = true,
    schema: String,
    saveMode: String,
    format: String,
    extraOptions: Option[Map[String, String]] = None,
    partitionBy: Option[List[String]] = None
)

/**
  * Writer model DTO
  * @param name name of the writer
  * @param datastoreModel DataStore model
  * @param options parameters map
  */
case class WriterModelDTO(
    name: String,
    datastoreModel: DatastoreModelDTO,
    options: Map[String, String]
)

/**
  * Streaming reader model DTO
  * @param name name of the reader
  * @param datastoreModel corresponding DataStore model
  * @param options parameters map
  * @param rateLimit incoming rate limit
  */
case class ReaderModelDTO(
    name: String,
    datastoreModel: DatastoreModelDTO,
    options: Map[String, String],
    rateLimit: Option[Int]
)

/**
  * Strategy DTO case classes
  */
sealed trait StrategyDTO

object StrategyDTO {
  val nifiType     = "nifi"
  val codebaseType = "codebase"
  val freecodeType = "freecode"
}

case class FreeCodeDTO(code: String, name: String, config: Option[JsObject]) extends StrategyDTO
case class FlowNifiDTO(processGroup: String, name: String, config: Option[JsObject]) extends StrategyDTO
case class StrategyClassDTO(className: String, config: Option[JsObject]) extends StrategyDTO
