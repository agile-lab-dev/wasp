package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, Cancellable}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.WaspMessageEnvelope
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, JsonConverter, StringToByteArrayUtil}

case object StopMainTask

case object StartMainTask

abstract class ProducerActor[T](val kafka_router: ActorRef, val topic: Option[TopicModel])
  extends Actor
    with Logging {

  implicit val system = context.system
  var task: Option[Cancellable] = None

  def generateOutputJsonMessage(input: T): String

  val generateOutputMessage: Option[(T) => Array[Byte]] = None

  def stopMainTask() = task.map(_.cancel())

  def mainTask()

  /**
    * Defines a function to extract the key to be used to identify the landing partition in kafka topic,
    * given a message of type T
    * @return a function that extract the partition key as String from the T instance to be sent to kafka
    */
  def retrievePartitionKey: T => String

  lazy val topicSchemaType = topic.get.topicDataType
  lazy val topicSchema = JsonConverter.toString(topic.get.schema.asDocument())

  override def postStop() {
    logger.info(s"Stopping actor ${this.getClass.getName}")
    stopMainTask()
  }

  override def receive: Actor.Receive = {
    case StopMainTask => stopMainTask()
    case StartMainTask => mainTask()
  }

  /**
   * Method to send to Kafka a specific message to be added to the raw topic and eventually to a custom topic.
   */
  def sendMessage(input: T) = {
    topic.foreach { p =>
      val msg = generateOutputJsonMessage(input)

      try {
        topicSchemaType match {
          case "avro" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, retrievePartitionKey(input), AvroToJsonUtil.jsonToAvro(msg, topicSchema))
          case "json" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, retrievePartitionKey(input), StringToByteArrayUtil.stringToByteArray(msg))
          case "plaintext" => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, retrievePartitionKey(input), StringToByteArrayUtil.stringToByteArray(input.asInstanceOf[String]))
          case _ => kafka_router ! WaspMessageEnvelope[String, Array[Byte]](p.name, retrievePartitionKey(input), AvroToJsonUtil.jsonToAvro(msg, topicSchema))
        }

      } catch {
        case e: Throwable => logger.error("Exception sending message to kafka", e)
      }
    }
  }
}