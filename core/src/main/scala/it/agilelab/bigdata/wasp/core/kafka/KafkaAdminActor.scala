package it.agilelab.bigdata.wasp.core.kafka

import akka.actor.{Actor, actorRef2Scala}
import it.agilelab.bigdata.wasp.core.logging.Logging
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkConnection
import org.I0Itec.zkclient.exception.ZkTimeoutException

object KafkaAdminActor {
  val name = "KafkaAdminActor"
  val topic = "test.topic"
  val sessionTimeout = 10000
  val connectionTimeout = 10000
  val partitions = 2
  val replicas = 1
}

class KafkaAdminActor extends Actor with Logging {

//  var zkClient: ZkClient = _
  var zkUtils: ZkUtils = _

  override def receive: Actor.Receive = {
    case message: AddTopic => call(message, addTopic)
    case message: CheckTopic => call(message, checkTopic)
    case message: RemoveTopic => call(message, removeTopic)
    case message: CheckOrCreateTopic => call(message, checkOrCreateTopic)
    case message: Initialization => call(message, initialization)
    case message: Any => logger.error("unknown message: " + message)
  }

  def initialization(message: Initialization): Boolean = {
    if (zkUtils != null) {
      logger.warn("Zookeeper client re-initialization, the before client will be close")
      zkUtils.close()
    }
    val kafkaConfig = message.kafkaConfigModel

    logger.info(s"Before create a zookeeper client with config: $kafkaConfig")
    try {
      val zkClient = ZkUtils.createZkClient(kafkaConfig.zookeeperConnections.toString, KafkaAdminActor.sessionTimeout, KafkaAdminActor.connectionTimeout)
      zkUtils = new ZkUtils(zkClient, new ZkConnection(kafkaConfig.zookeeperConnections.toString), false)
      logger.info(s"New zookeeper client created $zkClient")
      true
    } catch {
      case e: ZkTimeoutException =>
        val msg = s"Timeout during zookeeper connection, config: $kafkaConfig"
        logger.error(msg)
        val newException = new ZkTimeoutException(msg, e)
        sender() ! akka.actor.Status.Failure(newException)
        throw newException
      case e: Throwable =>
        logger.error(s"zkClient error $e")
        sender() ! akka.actor.Status.Failure(e)
        throw e
    }
  }

  override def postStop() = {

    if (zkUtils != null)
      zkUtils.close()

    zkUtils = null
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
      AdminUtils.createTopic(zkUtils, message.topic, message.partitions, message.replicas)
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
    AdminUtils.topicExists(zkUtils, message.topic)

  private def removeTopic(message: RemoveTopic): Boolean =
    try {
      AdminUtils.deleteTopic(zkUtils, message.topic)
      logger.info("Removed topic " + message.topic)
      true
    }
    catch {
      case throwable: Throwable =>
        logger.error("Error in topic '" + message.topic + "' creation")
        false
    }
}