package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Actor, actorRef2Scala}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ElasticConfigModel

import scala.util.{Failure, Success, Try}

object ElasticAdminActor {
  val name = "ElasticAdminActor"
  val dataType = "doc"
}

class ElasticAdminActor extends Actor with Logging {

  var elasticConfig: ElasticConfigModel = _
  var restClient: ElasticRestClient = _

  override def receive: Actor.Receive = {
    case message: AddAlias => call(message, addAlias)
    case message: AddIndex => call(message, addIndex)
    case message: AddMapping => call(message, addMapping)
    case message: CheckIndex => call(message, checkIndex)
    case message: RemoveAlias => call(message, removeAlias)
    case message: RemoveIndex => call(message, removeIndex)
    case message: CheckOrCreateIndex => call(message, checkOrCreateIndex)
    case message: Initialization => call(message, initialization)
    case message: Any => logger.error("unknown message: " + message)
  }

  def initialization(message: Initialization): Boolean = {
    if (restClient != null) {
      logger.warn("Elastic client re-initialization, the before client will be close")
      restClient.close()
    }

    val connections = message.elasticConfigModel
      .connections
      .filter(_.metadata.getOrElse(Map()).getOrElse("connectiontype", "") == "rest")

    val nodesAddress = connections.map(c => new InetSocketAddress(InetAddress.getByName(c.host), c.port))


    val unresolvedNodesAddress = nodesAddress.filter(_.isUnresolved)
    val resolvedNodesAddress = nodesAddress.filterNot(_.isUnresolved)

    logger.info(s"resolved nodes $resolvedNodesAddress")
    logger.info(s"un resolved nodes $unresolvedNodesAddress")

    if (resolvedNodesAddress.isEmpty) {
      val message = s"No nodes resolved $nodesAddress"
      logger.error(message)
      throw new Exception(message)
    }

    restClient = Try(new HighLevelElasticRestClient(resolvedNodesAddress.map(_.getHostName))) match {
      case Success(client) => client
      case Failure(e) => {
        val message = "Could not create Elasticsearch client"
        logger.error(message, e)
        throw new Exception(message, e)
      }
    }

    true
  }

  private def call[T <: ElasticAdminMessage](message: T, f: T => Any): Unit = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender() ! result
  }

  private def checkOrCreateIndex(message: CheckOrCreateIndex): Boolean = {
    var check = checkIndex(CheckIndex(message.index))

    if (!check)
      check = addIndex(AddIndex(message.index)) && addAlias(AddAlias(message.index, message.alias)) && addMapping(AddMapping(message.index, message.datatype, message.schema))

    check
  }

  private def addMapping(message: AddMapping): Boolean = {
    restClient.addMapping(message.index, message.datatype, message.schema)
  }

  private def addIndex(message: AddIndex): Boolean = {
    restClient.addIndex(message.index, None)
  }

  private def checkIndex(message: CheckIndex): Boolean = {
    restClient.checkIndex(message.index)
  }

  private def addAlias(message: AddAlias): Boolean = if (message.index == message.alias) {
    logger.info("received request to create index alias with same name as index")
    true
  } else {
    restClient.addAlias(message.index, message.alias)
  }


  private def removeAlias(message: RemoveAlias): Boolean = {
    restClient.removeAlias(message.index, message.alias)
  }

  private def removeIndex(message: RemoveIndex): Boolean = {
    restClient.removeIndex(message.index)
  }

  override def postStop(): Unit = {
    if (restClient != null)
      restClient.close()

    restClient = null
    logger.info("Elastic  client stopped")
  }

}