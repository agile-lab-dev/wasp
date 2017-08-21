package it.agilelab.bigdata.wasp.core.solr

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.SolrConfigModel
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.client.solrj.response.{CollectionAdminResponse, QueryResponse}
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.cloud.{ClusterState, ZkStateReader}
import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Try


object SolrAdminActor {
  val name = "SolrAdminActor"
  val collection = "wasp-def-collection"
  val alias = "wasp-alias"
  val configSet = "waspConfigSet"
  val template = "managedTemplate"
  val numShards = 1
  val replicationFactor = 1
  val schema =
    """[
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

class SolrAdminActor extends Actor with SprayJsonSupport with DefaultJsonProtocol with Logging {

  var solrConfig: SolrConfigModel = _
  var solrServer: CloudSolrServer = _
  //TODO prendere il timeout dalla configurazione
  //implicit val timeout = Timeout(ConfigManager.config)
  implicit val timeout = WaspSystem.generalTimeout

  implicit val materializer = ActorMaterializer()
  implicit val system = this.context.system

  //http://limansky.me/posts/2016-04-30-easy-json-analyze-with-spray-json.html
  class JsFieldOps(val field: Option[JsValue]) {
    def \(name: String) = field map (_ \ name) getOrElse this

    def ===(x: JsValue) = field.contains(x)

    def =!=(x: JsValue) = !field.contains(x)
  }

  implicit class JsValueOps(val value: JsValue) {
    def \(name: String) = new JsFieldOps(Try(value.asJsObject).toOption.flatMap(_.fields.get(name)))
  }


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
          s"${connection.host}:${connection.port}${
            connection.metadata.flatMap(_.get("zookeeperRootNode"))
              .getOrElse("")
          }")
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
      manageConfigSet(
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

    solrServer = null
    logger.debug("Solr - client stopped")
  }

  private def call[T <: SolrAdminMessage](message: T, f: T => Any) = {
    val result = f(message)
    logger.info(message + ": " + result)
    sender ! result
  }

  private def manageConfigSet(name: String,
                              template: String) = {

    val uri = s"${solrConfig.apiEndPoint.get.toString()}/admin/configs?action=DELETE&name=$name&baseConfigSet=$template&configSetProp.immutable=false&wt=json"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(uri = uri)
        .withHeaders(RawHeader("Content-Type", "application/json"))
        .withHeaders(RawHeader("Accept", "application/json"))
    )
    responseFuture foreach { res =>
      res.status match {
        case OK =>
          Unmarshal(res.entity).to[JsValue].map { info: JsValue =>
            if ((info \ "responseHeader" \ "status").===(JsNumber(0))) {
              logger.info("Config Set Deleted")
              createConfigSet(name, template)
            } else if ((info \ "responseHeader" \ "status").===(JsNumber(400))) {
              logger.info("Config Set Doesn't Exists")
              createConfigSet(name, template)
              logger.info(s"The information for my ip is: $info")
            } else {
              logger.error("Solr Schema API Status Code NOT recognized")
            }
          }
        case _ =>
          Unmarshal(res.entity).to[String].map { body =>
            logger.info(s"Solr Schema API Status Code NOT recognized $body")
          }
      }
    }
  }

  private def createConfigSet(name: String,
                              template: String): Unit = {
    val uri = s"${solrConfig.apiEndPoint.get.toString()}/admin/configs?action=CREATE&name=$name&baseConfigSet=$template&configSetProp.immutable=false&wt=json"

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(uri = uri)
        .withHeaders(RawHeader("Content-Type", "application/json"))
        .withHeaders(RawHeader("Accept", "application/json"))
    )

    responseFuture.foreach { res =>
      res.status match {
        case OK =>
          Unmarshal(res.entity).to[JsValue].map { info: JsValue =>
            if ((info \ "responseHeader" \ "status").===(JsNumber(0))) {
              logger.info("Config Set Created")
            } else if ((info \ "responseHeader" \ "status").===(JsNumber(400))) {
              logger.info("Config Set Doesn't Exists")
            } else {
              logger.error("Solr - Config Set NOT Created")
            }
          }
        case _ =>
          Unmarshal(res.entity).to[String].map { body =>
            logger.error("Solr - Config Set NOT Created")
            logger.error(s"Solr Schema API Status Code NOT recognized $body")
          }
      }
    }
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

    val uri = s"${solrConfig.apiEndPoint.get.toString()}/${message.collection}/schema/fields"

    val jsonEntity = JsObject(
      "collection" -> JsString(message.collection),
      "schema" -> JsString(message.schema)
    ).toString()

    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(uri = uri)
        .withHeaders(RawHeader("Content-Type", "application/json"))
        .withHeaders(RawHeader("Accept", "application/json"))
        .withMethod(HttpMethods.POST)
        .withEntity(jsonEntity)
    )


    Await.result(responseFuture.map { res =>
      res.status match {
        case OK =>
          logger.info(s"Solr - Add Mapping response status ${res.status.value}, $message")
          true
        case _ =>
          logger.error(s"Solr - Schema NOT created, $message")
          false
      }
    }, timeout.duration)
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


