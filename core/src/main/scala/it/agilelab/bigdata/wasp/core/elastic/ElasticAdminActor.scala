package it.agilelab.bigdata.wasp.core.elastic

import akka.actor.{Actor, actorRef2Scala}
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.configuration.ElasticConfigModel
import it.agilelab.bigdata.wasp.core.utils.ElasticConfiguration
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object ElasticAdminActor {

  val name = "ElasticAdminActor"
  val alias = "testalias"
  val cluster = "wasp"
  val dataType = "testtype"
  val host = "localhost"
  val index = "testindex"
  val port = 9300
  val schema = """{
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

class ElasticAdminActor extends Actor {

  val logger = WaspLogger(classOf[ElasticAdminActor])
  var elasticConfig: ElasticConfigModel = _
  var transportClient: TransportClient = _

  def receive: Actor.Receive = {

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
    val settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticConfig.cluster_name).build()

    transportClient = new TransportClient(settings)
    logger.debug(s"New elastic client created with settings: $settings and config $elasticConfig")

    for (connection <- elasticConfig.connections.filter(_.metadata.flatMap(_.get("connectiontype")).getOrElse("") == "binary")) {

      val address = new InetSocketTransportAddress(connection.host, connection.port)
      if(address.address().isUnresolved){
        val msg = s"Impossible to resolve connection: ${connection.host}:${connection.port}"
        logger.warn(msg)
      }else {
        try {
          transportClient.addTransportAddress(address)
          logger.debug("added elastic node '" + connection.host + ":" + connection.port + "'")
        }
        catch {
          case e: Throwable => {
            val msg = s"Fail during elastic transportClient initialization; config: $elasticConfig, msg: ${e.getMessage}"
            logger.error(msg)
            transportClient.removeTransportAddress(address)
          }
        }
      }

    }

    if (transportClient.connectedNodes().isEmpty) {
      throw new Exception(s"There is NO nodes in the elastic transportClient, config: $elasticConfig")
    }

    true
  }

  override def postStop() = {
    for (connection <- elasticConfig.connections) {
      val address = new InetSocketTransportAddress(connection.host, connection.port)
      transportClient.removeTransportAddress(address)
      logger.debug("removed elastic node '" + connection.host + ":" + connection.port + "'")
    }

    if (transportClient != null)
      transportClient.close()

    transportClient = null
    logger.debug("elastic client stopped")
  }

  private def call[T <: ElasticAdminMessage](message: T, f: T => Any) = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender ! result
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
    builder.setType(message.datatype)
    builder.setSource(message.schema)
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
    val builder = transportClient.admin.indices.prepareDeleteMapping(message.index)
    builder.setType(message.datatype)
    logger.info("Removing mapping: " + message.datatype)
    builder.execute.actionGet.isAcknowledged
  }

  private def search(message: Search): SearchResponse = {
    val search = transportClient.prepareSearch(message.index)
    val query = message.query match {
      case None => search
      case Some(q) => search.setQuery(q)
    }
    val sortedQuery = message.sort match {
      case None => query
      case Some(q) => query.addSort(message.sort.get)
    }
    sortedQuery.setSize(message.size).setFrom(message.from).execute().actionGet()
  }
}