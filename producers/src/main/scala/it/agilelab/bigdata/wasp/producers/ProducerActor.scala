package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, Cancellable}
import it.agilelab.bigdata.wasp.core.SystemPipegraphs._
import it.agilelab.bigdata.wasp.core.WaspEvent.WaspMessageEnvelope
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, JsonConverter, JsonToByteArrayUtil}
import org.mongodb.scala.bson.BsonDocument
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsValue}

case object StopMainTask

case object StartMainTask

abstract class ProducerActor[T](val kafka_router: ActorRef, val topic: Option[TopicModel]) extends Actor  with Logging {
  implicit val system = context.system
  var task: Option[Cancellable] = None

  def generateRawOutputJsonMessage(input: T): Map[String, JsValue]

  def generateOutputJsonMessage(input: T): Map[String, JsValue]

  private def decorateJsonWithMetadata(input: T, f: (T) => Map[String, JsValue] ): Map[String, JsValue] = {
    val res = f(input)

    if(res.get("metadata").isEmpty) {
      val v = Vector(JsObject("name" -> JsString(""), "ts" -> JsNumber(0L)))

      res + ("metadata" -> JsObject("id" -> JsString(""),
        "arrivalTimestamp" -> JsNumber(0L),
        "lat" -> JsNumber(0D),
        "lon" -> JsNumber(0D),
        "lastSeenTimestamp" -> JsNumber(0L),
        "path" -> JsArray(v)))
    }

    else {
      logger.warn("Attention! Has been defined a metadata field that the framework uses internally")
      res
    }

  }

  def stopMainTask() = task.map(_.cancel())

  def mainTask()

  //TODO occhio che abbiamo la partition key schianatata, quindi usiamo sempre e solo una partizione
  val partitionKey = "partitionKey"

  val rawTopicSchema = JsonConverter.toString(rawTopic.schema.getOrElse(BsonDocument()).asDocument())
  lazy val topicSchemaType = topic.get.topicDataType
  lazy val topicSchema = JsonConverter.toString(topic.get.schema.getOrElse(BsonDocument()).asDocument())

  override def postStop() {
    logger.info(s"Stopping actor ${this.getClass.getName}")
    stopMainTask()
    super.postStop()
  }

  def receive: Actor.Receive = {
    case StopMainTask => stopMainTask()
    case StartMainTask => mainTask()
  }

  /**
   * Method to send to Kafka a specific message to be added to the raw topic and eventually to a custom topic.
   *
   * System pipelines won't write to the raw topic.
   */
  def sendMessage(input: T) = {

    if (topic.isEmpty) {
      val msg = decorateJsonWithMetadata(input, generateRawOutputJsonMessage)
      val rawJson = JsObject(msg).toString()
      //TODO: Add rawSchema from system raw pipeline
      try {
        topicSchemaType match {
          case "avro" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](rawTopic.name, partitionKey, AvroToJsonUtil.jsonToAvro(rawJson, rawTopicSchema))
          case "json" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](rawTopic.name, partitionKey, JsonToByteArrayUtil.jsonToByteArray(rawJson))
          case _ => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](rawTopic.name, partitionKey, AvroToJsonUtil.jsonToAvro(rawJson, rawTopicSchema))
        }

      } catch {
        case e: Throwable => logger.error("Exception sending message to kafka", e)
      }
    }

    topic.foreach { p =>
      //decorate with metadata field
      val msg = decorateJsonWithMetadata(input, generateOutputJsonMessage)
      val customJson = JsObject(msg).toString()
      try {
        topicSchemaType match {
          case "avro" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, partitionKey, AvroToJsonUtil.jsonToAvro(customJson, topicSchema))
          case "json" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, partitionKey, JsonToByteArrayUtil.jsonToByteArray(customJson))
          case _ => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, partitionKey, AvroToJsonUtil.jsonToAvro(customJson, topicSchema))
        }

      } catch {
        case e: Throwable => logger.error("Exception sending message to kafka", e)
      }
    }
  }
}