package it.agilelab.bigdata.wasp.consumers.spark.plugins.solr

import java.{lang, util}
import java.util.Properties

import akka.actor.Actor
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.ActorMaterializer
import com.lucidworks.spark.util.SolrSupport
import com.lucidworks.spark.util.SolrSupport.CloudClientParams
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.SolrConfigModel
import org.apache.http.client.HttpClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpClientUtil, Krb5HttpClientBuilder}
import org.apache.solr.client.solrj.request.schema.SchemaRequest
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, ConfigSetAdminRequest}
import org.apache.solr.client.solrj.response.{CollectionAdminResponse, ConfigSetAdminResponse, QueryResponse}
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.cloud.{ClusterState, ZkStateReader}
import org.apache.solr.common.params.MapSolrParams
import spray.json.DefaultJsonProtocol

import scala.collection.JavaConverters._
import scala.collection.mutable

object SolrAdminActor {
  val name              = "SolrAdminActor"
  val collection        = "wasp-def-collection"
  val alias             = "wasp-alias"
  val configSet         = "waspConfigSet"
  val template          = "schemalessTemplate"
  val numShards         = 1
  val replicationFactor = 1
  val schema            = ""
}

class SolrAdminActor extends Actor with SprayJsonSupport with DefaultJsonProtocol with Logging {

  var solrConfig: SolrConfigModel = _
  var solrServer: CloudSolrClient = _
  var httpClient: HttpClient      = _

  implicit val materializer = ActorMaterializer()
  implicit val system       = this.context.system

  override def receive: Actor.Receive = {
    case message: Search                  => call(message, search)
    case message: AddCollection           => call(message, addCollection)
    case message: AddMapping              => call(message, addMapping)
    case message: AddAlias                => call(message, addAlias)
    case message: RemoveCollection        => call(message, removeCollection)
    case message: RemoveAlias             => call(message, removeAlias)
    case message: Initialization          => call(message, initialization)
    case message: CheckOrCreateCollection => call(message, checkOrCreateCollection)
    case message: CheckCollection         => call(message, checkCollection)
    case message: Any                     => logger.error(s"Unknown message: $message")
  }

  def initialization(message: Initialization): Boolean = {

    if (solrServer != null) {
      logger.warn("Solr - Client re-initialization, the before client will be close")
      solrServer.close()
    }

    solrConfig = message.solrConfigModel

    logger.info(s"Solr - New client created with: config $solrConfig")
    val jaasPath = System.getProperty("java.security.auth.login.config")
    logger.info(s"Solr Kerberos is enabled with Jaas Path -> ${System.getProperty("java.security.auth.login.config")}")

    if (jaasPath != null) {
      HttpClientUtil.setHttpClientBuilder(new Krb5HttpClientBuilder().getBuilder)
    }
    httpClient = HttpClientUtil.createClient(new MapSolrParams(new util.HashMap[String, String]()))

    val solrClientBuilder = CloudClientParams(solrConfig.zookeeperConnections.toString)
    solrServer = SolrSupport.getNewSolrCloudClient(solrClientBuilder)

    try {
      solrServer.connect()
    } catch {
      case e: Throwable =>
        val msg = "Solr NOT connected!"
        logger.error(msg, e)
    }

    true
  }

  override def postStop(): Unit = {
    if (solrServer != null)
      solrServer.close()

    solrServer = null
    logger.info("Solr - client stopped")
  }

  private def call[T <: SolrAdminMessage](message: T, f: T => Any): Unit = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender() ! result
  }

  private def manageConfigSet(name: String, template: String) = {

    val listConfigSetRequest = new ConfigSetAdminRequest.List()

    val listConfigSetResponse = listConfigSetRequest.process(solrServer)

    val retErrors = listConfigSetResponse.getErrorMessages()

    if (retErrors != null && retErrors.size() > 0) {
      logger.error(s"Error listing config sets. ${retErrors.toString}")
      false
    } else {
      listConfigSetResponse.getConfigSets().contains(name) match {

        case true => {
          deleteConfigSet(name, template) match {
            case true  => createConfigSet(name, template)
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
    } else {
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
    } else {
      logger.info("Config Set Created.")
      true
    }
  }

  private def addCollection(message: AddCollection): Boolean = {

    logger.info(
      s"Add collection with name ${message.collection}, numShards ${message.numShards} and replica factor ${message.replicationFactor}"
    )

    val numShards         = message.numShards
    val replicationFactor = message.replicationFactor
    val configName        = s"${SolrAdminActor.configSet}_${message.collection}"

    val createRequest: CollectionAdminRequest.Create =
      CollectionAdminRequest.createCollection(message.collection, configName, numShards, replicationFactor)

    val createResponse: CollectionAdminResponse = createRequest.process(solrServer)

    val ret = createResponse.isSuccess()
    if (!ret)
      logger.info(s"Collection NOT successfully created. ${message.collection}")

    ret
  }

  private def addMapping(message: AddMapping): Boolean = {
    import spray.json._


    case class Type(name :String)
    case class Field (name: String,
      `type`: String,
    indexed: Boolean = true,
    stored: Boolean = true,
    docValues: Option[Boolean] = None,
    sortMissing: Option[String] = None,
    multiValued: Option[Boolean] = None,
    omitNorms: Option[Boolean] = None,
    omitTermFreqAndPositions: Option[Boolean] = None,
    omitPositions: Option[Boolean] = None,
    termVectors: Option[Boolean] = None,
    termPositions: Option[Boolean] = None,
    termOffsets: Option[Boolean] = None,
    termPayloads: Option[Boolean] = None,
    required: Boolean = false,
    useDocValuesAsStored: Option[Boolean] = None,
    large: Option[Boolean] = None)

    implicit  val fieldFormat: RootJsonFormat[Field] =  jsonFormat17(Field.apply)

    val read = JsonParser(message.schema).convertTo[Seq[Field]]

    val updates: Seq[SchemaRequest.Update] = read.map {

      case Field(
          name,
          t,
          indexed,
          stored,
          docValues,
          sortMissing,
          multiValued,
          omitNorms,
          omitTermFreqAndPositions,
          omitPositions,
          termVectors,
          termPositions,
          termOffsets,
          termPayloads,
          required,
          useDocValuesAsStored,
          large
          ) =>
        val map = mutable.Map[String, AnyRef]()

        sortMissing.foreach { v =>
            map += ("sortMissing" -> v)
        }
        map += ("name"     -> name)
        map += ("indexed"  -> new lang.Boolean(indexed))
        map += ("stored"   -> new lang.Boolean(stored))
        map += ("required" -> new lang.Boolean(required))
        map += ("type"     -> t)

        docValues.foreach(v => map += ("docValues"                               -> new java.lang.Boolean(v)))
        multiValued.foreach(v => map += ("multiValued"                           -> new java.lang.Boolean(v)))
        omitNorms.foreach(v => map += ("omitNorms"                               -> new java.lang.Boolean(v)))
        omitTermFreqAndPositions.foreach(v => map += ("omitTermFreqAndPositions" -> new java.lang.Boolean(v)))
        omitPositions.foreach(v => map += ("omitPositions"                       -> new lang.Boolean(v)))
        omitPositions.foreach(v => map += ("omitPositions"                       -> new lang.Boolean(v)))
        termVectors.foreach(v => map += ("termVectors"                           -> new lang.Boolean(v)))
        termPositions.foreach(v => map += ("termPositions"                       -> new lang.Boolean(v)))
        termOffsets.foreach(v => map += ("termOffsets"                           -> new lang.Boolean(v)))
        termPayloads.foreach(v => map += ("termPayloads"                         -> new lang.Boolean(v)))
        useDocValuesAsStored.foreach(v => map += ("useDocValuesAsStored"         -> new lang.Boolean(v)))
        large.foreach(v => map += ("large"                                       -> new lang.Boolean(v)))

        new SchemaRequest.AddField(map.asJava)
    }

    val schemaRequest = new SchemaRequest.MultiUpdate(updates.toList.asJava)

    val res = schemaRequest.process(solrServer, message.collection)

    if (res.getStatus != 200 && res.getStatus != 0) {
      logger.error("Cannot set schema for collection")
      false
    } else {
      true
    }
  }

  private def addAlias(message: AddAlias): Boolean = {
    logger.info(s"Add alias $message")

    val createRequest: CollectionAdminRequest.CreateAlias =
      CollectionAdminRequest.createAlias(message.alias, message.collection)

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

    val removeRequest: CollectionAdminRequest.Delete = CollectionAdminRequest.deleteCollection(message.collection)

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

    val removeRequest: CollectionAdminRequest.DeleteAlias = CollectionAdminRequest.deleteAlias(message.collection)

    val removeResponse: CollectionAdminResponse =
      removeRequest.process(solrServer)

    val ret = removeResponse.isSuccess
    if (!ret) {
      logger.info(s"Collection Alias NOT successfully removed. ${message.collection}")
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
            AddCollection(message.collection, message.numShards, message.replicationFactor)
          ) &&
          addMapping(
            AddMapping(message.collection, message.schema, message.numShards, message.replicationFactor)
          )

    check
  }

  private def checkCollection(message: CheckCollection): Boolean = {
    logger.info(s"Check collection: $message")

    val zkStateReader: ZkStateReader = solrServer.getZkStateReader
    zkStateReader.forciblyRefreshAllClusterStateSlow()

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
