package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, Cancellable}
import it.agilelab.bigdata.wasp.core.SystemPipegraphs._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.WaspMessageEnvelope
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, JsonConverter, JsonToByteArrayUtil}

case object StopMainTask

case object StartMainTask


abstract class ProducerActor[T](val kafka_router: ActorRef, val topic: Option[TopicModel]) extends Actor with Logging {
  implicit val system = context.system
  var task: Option[Cancellable] = None

  def generateRawOutputJsonMessage(input: T): String

  def generateOutputJsonMessage(input: T): String

  def stopMainTask() = task.map(_.cancel())

  def mainTask()

  //TODO occhio che abbiamo la partition key schianatata, quindi usiamo sempre e solo una partizione
  val partitionKey = "partitionKey"

  val rawTopicSchema = JsonConverter.toString(rawTopic.schema.asDocument())
  lazy val topicSchemaType = topic.get.topicDataType
  lazy val topicSchema = JsonConverter.toString(topic.get.schema.asDocument())

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
      val rawJson = generateRawOutputJsonMessage(input)
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
      val customJson = generateOutputJsonMessage(input)
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