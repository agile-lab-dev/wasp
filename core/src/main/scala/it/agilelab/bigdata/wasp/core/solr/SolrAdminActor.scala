package it.agilelab.bigdata.wasp.core.solr

import akka.actor.Actor
import com.ning.http.client.AsyncHttpClientConfig
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.configuration.SolrConfigModel
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.response.{CollectionAdminResponse, QueryResponse}
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.cloud.{ClusterState, ZkStateReader}
import play.api.libs.json._
import play.api.libs.ws._
import play.api.libs.ws.ning._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object SolrAdminActor {
  val name = "SolrAdminActor"
  val collection = "wasp-def-collection"
  val alias = "wasp-alias"
  val configSet = "waspConfigSet"
  val template = "managedTemplate"
  val numShards = 1
  val replicationFactor = 1
  val schema = """[
                        {
                            "name":"id_event",
                            "type":"tdouble",
                            "stored":true
                        },
                        {
                            "name":"source_name",
                            "type":"string",
                            "stored":true
                        },
                        {
                            "name":"topic_name",
                            "type":"string",
                            "stored":true
                        },
                        {
                            "name":"metric_name",
                            "type":"string",
                            "stored":true
                        },
                        {
                            "name":"timestamp",
                            "type":"tlong",
                            "stored":true
                        },
                        {
                            "name":"latitude",
                            "type":"tdouble",
                            "stored":true
                        },
                        {
                            "name":"longitude",
                            "type":"tdouble",
                            "stored":true
                        },
                        {
                            "name":"value",
                            "type":"string",
                            "stored":true
                        },
                        {
                            "name":"payload",
                            "type":"string",
                            "stored":true
                        }
                 ]"""

}

class SolrAdminActor extends Actor {

  val logger = WaspLogger(classOf[SolrAdminActor])
  var solrConfig: SolrConfigModel = _
  var solrServer: CloudSolrServer = _

  val builder = new AsyncHttpClientConfig.Builder()
  val wsClient: WSClient = new NingWSClient(builder.build())

  def receive: Actor.Receive = {
    case message: Search => call(message, search)
    case message: AddCollection => call(message, addCollection)
    case message: AddMapping => call(message, addMapping)
    case message: AddAlias => call(message, addAlias)
    case message: RemoveCollection => call(message, removeCollection)
    case message: RemoveAlias => call(message, removeAlias)
    case message: Initialization => call(message, initialization)
    case message: CheckOrCreateCollection =>
      call(message, checkOrCreateCollection)
    case message: CheckCollection => call(message, checkCollection)
    case message: Any => logger.error("unknown message: " + message)
  }

  def initialization(message: Initialization): Boolean = {

    if (solrServer != null) {
      logger.warn(
        s"Solr - Client re-initialization, the before client will be close")
      solrServer.shutdown()
    }

    solrConfig = message.solrConfigModel

    logger.debug(s"Solr - New client created with: config $solrConfig")

    solrServer = new CloudSolrServer(
      solrConfig.connections
        .map(connection =>
          s"${connection.host}:${connection.port}${connection.metadata.flatMap(_.get("zookeeperRootNode"))
            .getOrElse("")}")
        .mkString(","))

    try {
      solrServer.connect()
    } catch {
      case e: Exception => {
        logger.info(s"Solr NOT connected!")
        e.printStackTrace()
      }
      case _: Throwable => logger.info(s"Solr NOT connected!")
    }

    logger.info(s"Try to create a WASP ConfigSet.")

    try {
      manageConfigSet(wsClient,
        SolrAdminActor.configSet,
        SolrAdminActor.template)
    } catch {
      case _: Throwable => logger.info(s"manageConfigSet NOT Created. Go forward!")
    }

    true
  }

  override def postStop() = {
    if (solrServer != null)
      solrServer.shutdown()

    // close the client http connection.
    wsClient.close()

    solrServer = null
    logger.debug("Solr - client stopped")
  }

  private def call[T <: SolrAdminMessage](message: T, f: T => Any) = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender ! result
  }

  private def manageConfigSet(client: WSClient,
                              name: String,
                              template: String) = {

    assert(client != null)

    val response = client
      .url(s"${solrConfig.apiEndPoint.get
        .toString()}/admin/configs?action=DELETE&name=${name}&baseConfigSet=${template}&configSetProp.immutable=false&wt=json")
      .withHeaders("Content-Type" -> "application/json")
      .withHeaders("Accept" -> "application/json")
      .get

    response.foreach(r => {
      (r.json \ "responseHeader" \ "status").as[Int] match {
        case 0 =>
          logger.info("Config Set Deleted")
          createConfigSet(client, name, template)
        case 400 =>
          logger.info("Config Set Doesn't Exists")
          createConfigSet(client, name, template)
        case _ => logger.info("Solr Schema API Status Code NOT recognized")
      }
    })

  }

  private def createConfigSet(client: WSClient,
                              name: String,
                              template: String): Unit = {
    val response = client
      .url(s"${solrConfig.apiEndPoint.get
        .toString()}/admin/configs?action=CREATE&name=${name}&baseConfigSet=${template}&configSetProp.immutable=false&wt=json")
      .withHeaders("Content-Type" -> "application/json")
      .withHeaders("Accept" -> "application/json")
      .get

    response.foreach(r => {
      (r.json \ "responseHeader" \ "status").as[Int] match {
        case 0 => logger.info("Config Set Created")
        case 400 => logger.info("Config Set Doesn't Exists")
        case _ => logger.info("Solr - Config Set NOT Created")
      }
    })
  }

  private def addCollection(message: AddCollection): Boolean = {

    logger.info(s"AddCollection with name ${message.collection}, numShards ${message.numShards} and replica factor ${message.replicationFactor}.")

    val numShards = if (message.numShards > 0) message.numShards else SolrAdminActor.numShards
    val replicationFactor = if (message.replicationFactor > 0) message.replicationFactor else SolrAdminActor.replicationFactor

    val createRequest: CollectionAdminRequest.Create =
      new CollectionAdminRequest.Create()

    createRequest.setConfigName(SolrAdminActor.configSet)
    createRequest.setCollectionName(message.collection)
    createRequest.setNumShards(numShards)
    createRequest.setReplicationFactor(replicationFactor)

    val createResponse: CollectionAdminResponse =
      createRequest.process(solrServer)

    val ret = createResponse.isSuccess()
    if (!ret)
      logger.info(
        s"Collection NOT successfully created. ${message.collection}")

    ret
  }

  private def addMapping(message: AddMapping): Boolean = {

    val response = wsClient
      .url(s"${solrConfig.apiEndPoint.get.toString()}/${message.collection}/schema/fields")
      .withHeaders("Content-Type" -> "application/json")
      .withHeaders("Accept" -> "application/json")
      .post(Json.parse(message.schema))

    val r = Await.result(response, Duration.Inf)

    logger.info(
      s"Solr - Add Mapping response status ${r.status} - ${r.statusText}")

    val status = r.status
    if (status != 200)
      logger.info(s"Solr - Schema NOT created")

    (status == 200)
  }

  private def addAlias(message: AddAlias): Boolean = {
    val createRequest: CollectionAdminRequest.CreateAlias =
      new CollectionAdminRequest.CreateAlias()

    createRequest.setCollectionName(message.collection)
    createRequest.setAliasedCollections(message.alias)

    val createResponse: CollectionAdminResponse =
      createRequest.process(solrServer)

    val ret = createResponse.isSuccess()
    if (!ret)
      logger.info(
        s"Collection Alias NOT successfully created. ${message.collection}")

    ret
  }

  private def removeCollection(message: RemoveCollection): Boolean = {
    val removeRequest: CollectionAdminRequest.Delete =
      new CollectionAdminRequest.Delete()

    removeRequest.setCollectionName(message.collection)

    val removeResponse: CollectionAdminResponse =
      removeRequest.process(solrServer)

    val ret = removeResponse.isSuccess()
    if (!ret)
      logger.info(
        s"Collection NOT successfully removed. ${message.collection}")

    ret
  }

  private def removeAlias(message: RemoveAlias): Boolean = {
    val removeRequest: CollectionAdminRequest.DeleteAlias =
      new CollectionAdminRequest.DeleteAlias()

    removeRequest.setCollectionName(message.collection)

    val removeResponse: CollectionAdminResponse =
      removeRequest.process(solrServer)

    val ret = removeResponse.isSuccess()
    if (!ret)
      logger.info(
        s"Collection NOT successfully removed. ${message.collection}")

    ret
  }

  private def checkOrCreateCollection(
                                       message: CheckOrCreateCollection): Boolean = {
    var check = checkCollection(CheckCollection(message.collection))

    if (!check) {
      check = addCollection(AddCollection(message.collection, message.numShards, message.replicationFactor)) &&
              addMapping(AddMapping(message.collection, message.schema))
    }

    check
  }

  private def checkCollection(message: CheckCollection): Boolean = {
    val zkStateReader: ZkStateReader = solrServer.getZkStateReader()
    zkStateReader.updateClusterState(true)
    val clusterState: ClusterState = zkStateReader.getClusterState()

    val res = (clusterState.getCollectionOrNull(message.collection) != null)
    if (res)
      logger.info(s"The ${message.collection} exists.")

    res
  }

  private def search(message: Search): SolrDocumentList = {
    solrServer.setDefaultCollection(message.collection)

    val query: SolrQuery = new SolrQuery()
    query.setStart(message.from)
    query.setRows(message.size)

    message.query match {
      case None => query
      case Some(q) => q.map(v => query.setQuery(s"${v._1}:${v._2}"))
    }
    message.sort match {
      case None => query
      case Some(q) => q.map(v => query.setSort(v._1, v._2))
    }

    logger.debug(s"Performing this query: ${query}")

    val response: QueryResponse = solrServer.query(query)

    val list: SolrDocumentList = response.getResults()

    logger.debug(s"Doc. found count: ${list.size()} - ${list}")

    list
  }

}
