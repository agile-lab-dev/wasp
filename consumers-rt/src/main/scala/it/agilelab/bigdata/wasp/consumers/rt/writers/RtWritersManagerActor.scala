package it.agilelab.bigdata.wasp.consumers.rt.writers

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.camel.{CamelMessage, Producer}
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, TopicBL, WebsocketBL}
import it.agilelab.bigdata.wasp.core.elastic.CheckOrCreateIndex
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, BSONFormats, ConfigManager}
import org.apache.camel.component.websocket._
import play.libs.Json
import reactivemongo.bson.{BSONDocument, BSONString}

import scala.concurrent.{Await, Future}

class RtWritersManagerActor(env:{val topicBL: TopicBL; val websocketBL: WebsocketBL; val indexBL: IndexBL}, writer: Option[WriterModel]) extends Actor{
  private val log = WaspLogger(this.getClass.getName)

  val endpoint: Option[ActorRef] = if(writer.isDefined)
  {
    initializeEndpoint(writer.get)
  } else None

  override def receive: Receive = {
    case data : String => endpoint match{
      case Some(actor) =>
        actor ! data
        //TODO
      case None => //TODO: do nuffin is ok here?
    }
  }

  override def postStop() = {
    super.postStop()
    if (endpoint.isDefined)
      {
        endpoint.get ! PoisonPill
      }
  }

  def initializeEndpoint(writer: WriterModel): Option[ActorRef] = {
    writer.writerType.wtype match {
      case "topic" => Some(context.actorOf(Props(new CamelKafkaWriter(env.topicBL, writer))))
      case "index" => writer.writerType.product match {
        case "elastic" => {
          println("DEBUG: Starting a CamelElasticWriter")
          Some(context.actorOf(Props(new CamelElasticWriter(env.indexBL, writer))))
        }
        case "solr" => ??? // TODO
        case _ => ??? // TODO exception?
      }
      case "websocket" => Some(context.actorOf(Props(new CamelWebsocketWriter(env.websocketBL, writer))))
      case _ => None //TODO gestire writer inaspettato
    }
  }
}

class CamelKafkaWriter(topicBL: TopicBL, writer: WriterModel) extends Producer {

  //TODO: Configurazione completa?
  val kafkaTopicFut: Future[Option[TopicModel]] = topicBL.getById(writer.id.stringify)
  //TODO recuperare timeout wasp
  val kafkaTopic: TopicModel = Await.result(kafkaTopicFut, Timeout(30, TimeUnit.SECONDS).duration).get
  val topicSchema = BSONFormats.toString(kafkaTopic.schema.getOrElse(BSONDocument()))
  val topicDataType = kafkaTopic.topicDataType


  override def endpointUri: String = getKafkaUri(writer.name)

  def getKafkaUri(topic: String) = {
    val config = ConfigManager.getKafkaConfig
    val kafkaConnections = config.connections.mkString(",") // Why the "," https://github.com/apache/camel/blob/master/components/camel-kafka/src/test/java/org/apache/camel/component/kafka/KafkaComponentTest.java
    val zookeeperConnections = config.zookeeper

    s"kafka:$kafkaConnections?topic=$topic&zookeeperConnect=$zookeeperConnections&groupId=${this.hashCode().toString}"

  }

  override def transformOutgoingMessage(msg: Any): Any = {
    msg match {
      case m: String => topicDataType match {
        case "avro" => AvroToJsonUtil.jsonToAvro(m, topicSchema)
        case "json" => m.getBytes
      }
      case camel: CamelMessage => {
        camel
      }
      // TODO
      case default: Any => ???
    }
  }
}

class CamelElasticWriter(indexBL: IndexBL, writer: WriterModel) extends Producer {
  val indexConfigFut: Future[Option[IndexModel]] = indexBL.getById(writer.id.stringify)
  //TODO recuperare timeout wasp
  val indexConfigOpt: Option[IndexModel] = Await.result(indexConfigFut, Timeout(30, TimeUnit.SECONDS).duration)
  if (indexConfigOpt.isEmpty) {
    //TODO eccezione? fallisce l'attore?
  }
  val index = indexConfigOpt.get
  //TODO da verificare: anche import di WaspSystem.



  override def endpointUri: String = getElasticSearchUri

  def getElasticSearchUri: String = {
    val elasticConfig = ConfigManager.getElasticConfig
    val indexName = ConfigManager.buildTimedName(index.name)
    val ips = elasticConfig.connections.map(_.host).mkString(",")
    if (??[Boolean](WaspSystem.elasticAdminActor, CheckOrCreateIndex(indexName, index.name, index.dataType, BSONFormats.toString(index.schema.get)))) {
      //
    }
    val clusterName = ConfigManager.getElasticConfig.cluster_name
    //TODO se ci sono più connections assegnare così 'ips' va bene?
    s"elasticsearch://$clusterName?operation=INDEX&indexName=${indexName}&indexType=${index.dataType}&ip=${ips}"
  }

  override def transformOutgoingMessage(msg: Any): Any = {
    msg match {
      case m: String => {
        val json = Json.parse(m)
        val finalString = Json.stringify(json)
        finalString
      }
      case camel: CamelMessage => {
        camel
      }
        //TODO
      case default: Any => ???
    }

  }

}

class CamelWebsocketWriter(websocketBL: WebsocketBL, writer: WriterModel) extends Producer{

  val webSocketConfigFut: Future[Option[WebsocketModel]] = websocketBL.getById(writer.id.stringify)
  //TODO recuperare timeout wasp
  val webSocketConfigOpt: Option[WebsocketModel] = Await.result(webSocketConfigFut, Timeout(30, TimeUnit.SECONDS).duration)
  if (webSocketConfigOpt.isEmpty) {
    //TODO eccezione? fallisce l'attore?
  }

  override def endpointUri: String = getHttpUri

  override def postStop() = {
    //PROBLEMA: http://stackoverflow.com/questions/33152976/how-to-stop-the-websocketcomponent-in-apache-camel?noredirect=1
    super.postStop()
    this.camelContext.getComponent("websocket", classOf[WebsocketComponent]).doStop()
    this.camelContext.removeComponent("websocket")
  }

  def getHttpUri = {
    def getOptions(websocketConfig: BSONDocument): String = {
      websocketConfig.elements.map( x => x._1 + "=" + x._2.asInstanceOf[BSONString].value).mkString("&")
    }

    val rootUri = s"websocket://${webSocketConfigOpt.get.host}:${webSocketConfigOpt.get.port}/${webSocketConfigOpt.get.resourceName}"
    val finalUri = if(webSocketConfigOpt.get.options.isDefined) {
        rootUri + s"?${getOptions(webSocketConfigOpt.get.options.get)}"
    } else {
      rootUri
    }
    finalUri
  }
}
