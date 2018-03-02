package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import java.util.Properties

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem.servicesTimeout
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.SolrConfigModel
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, ConfigSetAdminRequest}
import org.apache.solr.client.solrj.response.{CollectionAdminResponse, ConfigSetAdminResponse, QueryResponse}
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.cloud.{ClusterState, ZkStateReader}
import spray.json.DefaultJsonProtocol

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object SolrAdminActor {
  val name = "SolrAdminActor"
  val collection = "wasp-def-collection"
  val alias = "wasp-alias"
  val configSet = "waspConfigSet"
  val template = "schemalessTemplate"
  val numShards = 1
  val replicationFactor = 1
  val schema = ""
}

class SolrAdminActor
  extends Actor
    with SprayJsonSupport
    with DefaultJsonProtocol
    with Logging {

  var solrConfig: SolrConfigModel = _
  var solrServer: CloudSolrServer = _

  implicit val materializer = ActorMaterializer()
  implicit val system = this.context.system

  override def receive: Actor.Receive = {
    case message: Search           => call(message, search)
    case message: AddCollection    => call(message, addCollection)
    case message: AddMapping       => call(message, addMapping)
    case message: AddAlias         => call(message, addAlias)
    case message: RemoveCollection => call(message, removeCollection)
    case message: RemoveAlias      => call(message, removeAlias)
    case message: Initialization   => call(message, initialization)
    case message: CheckOrCreateCollection => call(message, checkOrCreateCollection)
    case message: CheckCollection => call(message, checkCollection)
    case message: Any             => logger.error(s"Unknown message: $message")
  }

  def initialization(message: Initialization): Boolean = {

    if (solrServer != null) {
      logger.warn("Solr - Client re-initialization, the before client will be close")
      solrServer.shutdown()
    }

    solrConfig = message.solrConfigModel

    logger.info(s"Solr - New client created with: config $solrConfig")

    solrServer = new CloudSolrServer(solrConfig.zookeeperConnections.toString)

    try {
      solrServer.connect()
    } catch {
      case e: Throwable =>
        val msg = "Solr NOT connected!"
        logger.error(msg, e)
    }

    true
  }

  override def postStop() = {
    if (solrServer != null)
      solrServer.shutdown()

    solrServer = null
    logger.info("Solr - client stopped")
  }

  private def call[T <: SolrAdminMessage](message: T, f: T => Any) = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender() ! result
  }

  private def manageConfigSet(name: String, template: String) = {

    val listConfigSetRequest = new ConfigSetAdminRequest.List()

    val listConfigSetResponse = listConfigSetRequest.process(solrServer)

    val retErrors = listConfigSetResponse.getErrorMessages()

    if(retErrors != null && retErrors.size() > 0) {
      logger.error(s"Error listing config sets. ${retErrors.toString}")
      false
    }
    else {
      listConfigSetResponse.getConfigSets().contains(name) match {

        case true => {
          deleteConfigSet(name, template) match {
            case true => createConfigSet(name, template)
            case false => false
          }
        }

        case false => createConfigSet(name, template)
      }
    }
  }

  private def deleteConfigSet(name: String, template: String) = {

    val deleteConfigSetRequest = new ConfigSetAdminRequest.Delete()

    deleteConfigSetRequest.setConfigSetName(name)

    val deleteConfigSetResponse = deleteConfigSetRequest.process(solrServer)

    logger.info(s"Delete config set with name $name, template $template")

    val retErrors = deleteConfigSetResponse.getErrorMessages

    if (retErrors != null && retErrors.size() > 0) {
      logger.error(s"Collection NOT successfully created. ${retErrors.toString}")
      false
    }
    else {
      logger.info("Config Set Created.")
      true
    }
  }

  private def createConfigSet(name: String, template: String): Boolean = {

    val createConfigSetRequest = new ConfigSetAdminRequest.Create()

    createConfigSetRequest.setConfigSetName(name)
    createConfigSetRequest.setBaseConfigSetName(template)

    val prop = new Properties()
    prop.setProperty("immutable", "false")
    createConfigSetRequest.setNewConfigSetProperties(prop)

    val createConfigSetResponse: ConfigSetAdminResponse = createConfigSetRequest.process(solrServer)

    logger.info(s"Create config set with name $name, template $template")

    val retErrors = createConfigSetResponse.getErrorMessages

    if (retErrors != null && retErrors.size() > 0) {
      logger.error(s"Collection NOT successfully created. ${retErrors.toString}")
      false
    }
    else {
      logger.info("Config Set Created.")
      true
    }
  }

  private def addCollection(message: AddCollection): Boolean = {

    logger.info(
      s"Add collection with name ${message.collection}, numShards ${message.numShards} and replica factor ${message.replicationFactor}")

    val numShards = message.numShards
    val replicationFactor = message.replicationFactor

    val createRequest: CollectionAdminRequest.Create =
      new CollectionAdminRequest.Create()

    createRequest.setConfigName(s"${SolrAdminActor.configSet}_${message.collection}")
    createRequest.setCollectionName(message.collection)
    createRequest.setNumShards(numShards)
    createRequest.setReplicationFactor(replicationFactor)

    val createResponse: CollectionAdminResponse =
      createRequest.process(solrServer)

    val ret = createResponse.isSuccess()
    if (!ret)
      logger.info(s"Collection NOT successfully created. ${message.collection}")

    ret
  }

  private def collectionNameWShardsAndReplica(collectionName: String,
                                              numShards: Int,
                                              replicationFactor: Int) =
    s"${collectionName}_shard${numShards}_replica${replicationFactor}"

  private def addMapping(message: AddMapping): Boolean = {

    val zkStateReader = solrServer.getZkStateReader

    val liveNodes = zkStateReader.getClusterState.getLiveNodes.asScala.toSeq

    logger.info(s"Retrieved live-nodes from ZooKeeper: $liveNodes")

    if (liveNodes.isEmpty) {
      val message = "No live-nodes retrieved from ZooKeeper"
      logger.error(message)
      throw new Exception(message)
    }

    val liveNodeHead = zkStateReader.getBaseUrlForNodeName(liveNodes.head)

    val uri =
      s"$liveNodeHead/${collectionNameWShardsAndReplica(message.collection, message.numShards, message.replicationFactor)}/schema/fields"

    logger.info(s"$message, uri: '$uri'")

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(uri = uri)
        .withHeaders(RawHeader("Content-Type", "application/json"))
        .withHeaders(RawHeader("Accept", "application/json"))
        .withMethod(HttpMethods.POST)
        .withEntity(ContentTypes.`application/json`, message.schema)
    )

    Await.result(
      responseFuture.map { res =>
        res.status match {
          case OK => {
            logger.info(
              s"Solr - Add Mapping response status ${res.status}, $message")
            true
          }
          case _ => {
            logger.error(s"Solr - Schema NOT created, $message status ${res.status}")
            false
          }
        }
      }, servicesTimeout.duration
    )
  }

  private def addAlias(message: AddAlias): Boolean = {
    logger.info(s"Add alias $message")

    val createRequest: CollectionAdminRequest.CreateAlias =
      new CollectionAdminRequest.CreateAlias()

    createRequest.setCollectionName(message.collection)
    createRequest.setAliasedCollections(message.alias)

    val createResponse: CollectionAdminResponse =
      createRequest.process(solrServer)

    val ret = createResponse.isSuccess
    if (!ret) {
      logger.warn(s"Collection Alias NOT successfully created. ${message.collection}")
    }

    ret
  }

  private def removeCollection(message: RemoveCollection): Boolean = {
    logger.info(s"Remove collection $message")

    val removeRequest: CollectionAdminRequest.Delete =
      new CollectionAdminRequest.Delete()

    removeRequest.setCollectionName(message.collection)

    val removeResponse: CollectionAdminResponse =
      removeRequest.process(solrServer)

    val ret = removeResponse.isSuccess
    if (!ret) {
      logger.info(s"Collection NOT successfully removed. ${message.collection}")
    }

    ret
  }

  private def removeAlias(message: RemoveAlias): Boolean = {
    logger.info(s"Remove alias $message")

    val removeRequest: CollectionAdminRequest.DeleteAlias =
      new CollectionAdminRequest.DeleteAlias()

    removeRequest.setCollectionName(message.collection)

    val removeResponse: CollectionAdminResponse =
      removeRequest.process(solrServer)

    val ret = removeResponse.isSuccess
    if (!ret) {
      logger.info(s"Collection NOT successfully removed. ${message.collection}")
    }

    ret
  }

  private def checkOrCreateCollection(message: CheckOrCreateCollection): Boolean = {
    logger.info(s"Check or create collection: $message")

    var check = checkCollection(CheckCollection(message.collection))

    if (!check)
      check =
        manageConfigSet(s"${SolrAdminActor.configSet}_${message.collection}", SolrAdminActor.template) &&
        addCollection(
          AddCollection(
            message.collection,
            message.numShards,
            message.replicationFactor)
        ) &&
        addMapping(
          AddMapping(
            message.collection,
            message.schema,
            message.numShards,
            message.replicationFactor)
        )

    check
  }

  private def checkCollection(message: CheckCollection): Boolean = {
    logger.info(s"Check collection: $message")

    val zkStateReader: ZkStateReader = solrServer.getZkStateReader
    zkStateReader.updateClusterState(true)
    val clusterState: ClusterState = zkStateReader.getClusterState

    val res = clusterState.getCollectionOrNull(message.collection) != null
    if (res) {
      logger.info(s"The ${message.collection} exists.")
    }

    res
  }

  private def search(message: Search): SolrDocumentList = {
    logger.debug(s"Solr search: $message")

    solrServer.setDefaultCollection(message.collection)

    val query: SolrQuery = new SolrQuery()
    query.setStart(message.from)
    query.setRows(message.size)

    message.query match {
      case None    => query
      case Some(q) => q.map(v => query.setQuery(s"${v._1}:${v._2}"))
    }
    message.sort match {
      case None    => query
      case Some(q) => q.map(v => query.setSort(v._1, v._2))
    }

    logger.debug(s"Performing this query: $query")

    val response: QueryResponse = solrServer.query(query)

    val list: SolrDocumentList = response.getResults

    logger.debug(s"Doc. found count: ${list.size()} - $list")

    list
  }

}