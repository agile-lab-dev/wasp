package it.agilelab.bigdata.wasp.core.kafka

import akka.actor.{Actor, actorRef2Scala}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.KafkaEntryConfig
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import java.util.{Collections, Properties}
import scala.collection.Seq

object NewKafkaAdminActor {
  val name = "KafkaAdminActor"
  val topic = "test.topic"
  val sessionTimeout = 10000
  val connectionTimeout = 10000
  val partitions = 2
  val replicas = 1
}

class NewKafkaAdminActor extends Actor with Logging {

  var adminClient: AdminClient = _

  override def receive: Actor.Receive = {
    case message: AddTopic => call(message, addTopic)
    case message: CheckTopic => call(message, checkTopic)
    case message: RemoveTopic => call(message, removeTopic)
    case message: CheckOrCreateTopic => call(message, checkOrCreateTopic)
    case message: Initialization => call(message, initialization)
    case message: Any => logger.error("unknown message: " + message)
  }

  def initialization(message: Initialization): Boolean = {

    val kafkaConfig = message.kafkaConfigModel

    logger.info(s"Before create a zookeeper client with config: $kafkaConfig")
    try {
      val prop = createConfig(kafkaConfig.connections.map(_.toString).toSet, kafkaConfig.others)
      adminClient = AdminClient.create(prop)
      logger.info(s"New kafka client created $adminClient")
      true
    } catch {
      case e: Throwable =>
        logger.error(s"KafkaAdminClient error $e")
        sender() ! akka.actor.Status.Failure(e)
        throw e
    }
  }

  override def postStop() = {

    if (adminClient != null)
      adminClient.close()

    adminClient = null
    logger.debug("zookeeper client stopped")
  }

  private def call[T <: KafkaAdminMessage](message: T, f: T => Any) = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender() ! result
  }

  private def checkOrCreateTopic(message: CheckOrCreateTopic): Boolean = {
    logger.info(s"checkTopic , $message")
    var check = checkTopic(CheckTopic(message.topic))
    logger.info(s"checkOrCreateTopic $check , $message")
    if (!check)
      check = addTopic(AddTopic(message.topic, message.partitions, message.replicas))

    check
  }

  private def addTopic(message: AddTopic): Boolean =
    try {
      val newTopic = new NewTopic(message.topic, message.partitions, message.replicas.toShort)
      adminClient.createTopics(Collections.singleton(newTopic)).all().get()
      logger.info("Created topic " + message.topic)
      true
    }
    catch {
      case throwable: Throwable =>
        val msg = s"Error in topic '${message.topic}' creation"
        logger.error(msg, throwable)
        false
    }

  private def checkTopic(message: CheckTopic): Boolean =
    try {
      adminClient.listTopics().names().get().contains(message.topic)
    } catch {
      case throwable: Throwable =>
        logger.error("List topic '" + message.topic + "' error", throwable)
        false
    }

  private def removeTopic(message: RemoveTopic): Boolean =
    try {
      adminClient.deleteTopics(Collections.singleton(message.topic)).all().get()
      logger.info("Removed topic " + message.topic)
      true
    }
    catch {
      case throwable: Throwable =>
        logger.error("Error in topic '" + message.topic + "' creation", throwable)
        false
    }

  private def createConfig(brokers: Set[String], others: Seq[KafkaEntryConfig]): Properties = {

    val props = new Properties()
    props.put("bootstrap.servers", brokers.mkString(","))

    others.foreach(v => {
      props.put(v.key, v.value)
    })

    props
  }
}
