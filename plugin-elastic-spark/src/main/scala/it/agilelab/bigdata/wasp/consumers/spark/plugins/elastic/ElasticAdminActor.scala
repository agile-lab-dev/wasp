package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Actor, actorRef2Scala}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.ElasticConfigModel
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.client.PreBuiltTransportClient

object ElasticAdminActor {

  val name = "ElasticAdminActor"
  val alias = "testalias"
  val cluster = "wasp"
  val dataType = "testtype"
  val host = "localhost"
  val index = "testindex"
  val port = 9300
  val schema =
    """{
    "dataType" : { 
      "properties" : { 
        "id_event" : {
          "type" : "double", 
          "index" : "not_analyzed" 
        }, 
        "source_name" : {
          "type" : "string",
          "index" : "not_analyzed"
        }, 
        "topic_name" : {
          "type" : "string",
          "index" : "not_analyzed"
        },
        "metric_name" : {
          "type" : "string",
          "index" : "not_analyzed"
        },  
        "timestamp" : {
          "type" : "long",
          "index" : "not_analyzed"
        }, 
        "latitude" : {
          "type" : "double",
          "index" : "not_analyzed"
        },
        "longitude" : {
          "type" : "double",
          "index" : "not_analyzed"
        },  
        "value" : { " 
          "type" : "string",
          "index" : "not_analyzed"
        }, 
        "payload" : {
          "type" : "string",
          "index" : "not_analyzed"
        } 
      } 
    } 
  }"""


}

class ElasticAdminActor extends Actor with Logging {

  var elasticConfig: ElasticConfigModel = _
  var transportClient: TransportClient = _

  override def receive: Actor.Receive = {
    case message: AddAlias => call(message, addAlias)
    case message: AddIndex => call(message, addIndex)
    case message: AddMapping => call(message, addMapping)
    case message: CheckIndex => call(message, checkIndex)
    case message: RemoveAlias => call(message, removeAlias)
    case message: RemoveIndex => call(message, removeIndex)
    case message: Search => call(message, search)
    case message: CheckOrCreateIndex => call(message, checkOrCreateIndex)
    case message: Initialization => call(message, initialization)
    case message: Any => logger.error("unknown message: " + message)
  }

  def initialization(message: Initialization): Boolean = {
    if (transportClient != null) {
      logger.warn(s"Elastic client re-initialization, the before client will be close")
      transportClient.close()
    }

    elasticConfig = message.elasticConfigModel
    val settings = Settings.builder().put("cluster.name", elasticConfig.cluster_name).build()

    transportClient = new PreBuiltTransportClient(settings)
    logger.info(s"New elastic client created with settings: $settings and config $elasticConfig")

    logger.info(s"${transportClient.listedNodes()}")

    for (connection <- elasticConfig.connections.filter(_.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "binary")) {
      val address = new InetSocketTransportAddress(InetAddress.getByName(connection.host), connection.port)
      if (address.address().isUnresolved) {
        logger.warn(s"Impossible to resolve connection: ${connection.host}:${connection.port}")
      } else {
        try {
          transportClient.addTransportAddress(address)
          logger.info(s"${transportClient.listedNodes()}")
          logger.debug("added elastic node '" + connection.host + ":" + connection.port + "'")
        }
        catch {
          case e: Throwable => {
            logger.error(s"Fail during elastic transportClient initialization; config: $elasticConfig, msg: ${e.getMessage}")
            transportClient.removeTransportAddress(address)
          }
        }
      }

    }

    logger.info(s"${transportClient.listedNodes()}")

//    if (transportClient.connectedNodes().isEmpty) {
//      logger.error(s"There is NO nodes in the elastic transportClient, config: $elasticConfig")
//      throw new Exception(s"There is NO nodes in the elastic transportClient, config: $elasticConfig")
//    }

    true
  }

  override def postStop(): Unit = {
    for (connection <- elasticConfig.connections) {
      val address = new InetSocketTransportAddress(InetAddress.getByName(connection.host), connection.port)
      transportClient.removeTransportAddress(address)
      logger.debug("removed elastic node '" + connection.host + ":" + connection.port + "'")
    }

    if (transportClient != null)
      transportClient.close()

    transportClient = null
    logger.info("Elastic  client stopped")
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

  private def addAlias(message: AddAlias): Boolean = {
    val builder = transportClient.admin.indices.prepareAliases
    builder.addAlias(message.index, message.alias)
    logger.info("Creating alias: " + message.alias)
    builder.execute.actionGet.isAcknowledged
  }

  private def addIndex(message: AddIndex): Boolean = {
    val builder = transportClient.admin.indices.prepareCreate(message.index)
    logger.info("Creating index: " + message.index)
    builder.execute.actionGet.isAcknowledged
  }

  private def addMapping(message: AddMapping): Boolean = {
    val builder = transportClient.admin.indices.preparePutMapping(message.index)
    builder.setSource(message.datatype, XContentType.JSON)
    builder.setSource(message.schema, XContentType.JSON)
    logger.info("Creating mapping: " + message.datatype)
    builder.execute.actionGet.isAcknowledged
  }

  private def checkIndex(message: CheckIndex): Boolean = {
    val builder = transportClient.admin().indices().prepareExists(message.index)
    builder.execute().actionGet().isExists
  }

  private def removeAlias(message: RemoveAlias): Boolean = {
    val builder = transportClient.admin.indices.prepareAliases
    builder.removeAlias(message.index, message.alias)
    logger.info("Removing alias: " + message.alias)
    builder.execute.actionGet.isAcknowledged
  }

  private def removeIndex(message: RemoveIndex): Boolean = {
    val builder = transportClient.admin.indices.prepareDelete(message.index)
    logger.info("Removing index: " + message.index)
    builder.execute.actionGet.isAcknowledged
  }

  private def removeMapping(message: RemoveMapping): Boolean = {
    val builder = transportClient.admin.indices.prepareDelete(message.index)
    builder
    // TODO builder.setType(message.datatype)
    logger.info("Removing mapping: " + message.datatype)
    builder.execute.actionGet.isAcknowledged
  }

  private def search(message: Search): SearchResponse = {
    val search = transportClient.prepareSearch(message.index)
    val query = message.query match {
      case None => search
      case Some(q) => search.setQuery(q)
    }
    val sortedQuery: SearchRequestBuilder = message.sort match {
      case None => query
      case Some(q) => query.addSort("message", message.sort.get.order())
    }
    sortedQuery.setSize(message.size).setFrom(message.from).execute().actionGet()
  }
}