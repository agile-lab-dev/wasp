package it.agilelab.bigdata.wasp.consumers.rt.writers

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.camel.{CamelMessage, Producer}
import it.agilelab.bigdata.wasp.core.bl.{IndexBL, TopicBL, WebsocketBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, ConfigManager}
import org.apache.camel.component.websocket._
import org.mongodb.scala.bson.BsonDocument

import scala.collection.JavaConverters._


class RtWritersManagerActor(env: {
                                  val topicBL: TopicBL
                                  val websocketBL: WebsocketBL
                                  val indexBL: IndexBL
                                 },
                            writer: Option[WriterModel])
  extends Actor
    with Logging {

  val endpoint: Option[ActorRef] = if (writer.isDefined)
  {
    initializeEndpoint(writer.get)
  } else None

  override def receive: Actor.Receive = {
    case data : String => endpoint match {
      case Some(actor) =>
        actor ! data
        //TODO
      case None => //TODO: do nuffin is ok here?
    }
  }

  override def postStop() = {
    if (endpoint.isDefined)
      {
        endpoint.get ! PoisonPill
      }
  }

  def initializeEndpoint(writer: WriterModel): Option[ActorRef] = {
    writer.writerType.category match {
      case Datastores.topicCategory => Some(context.actorOf(Props(new CamelKafkaWriter(env.topicBL, writer))))
      case Datastores.indexCategory => writer.writerType.getActualProduct match {
        case Datastores.elasticProduct => ???
        //TODO Migrate to the RT plugin
        //logger.debug("Starting a CamelElasticWriter")
        //Some(context.actorOf(Props(new CamelElasticWriter(env.indexBL, writer))))

        case Datastores.solrProduct => ??? // TODO
        case _ => ??? // TODO exception?
      }
      case Datastores.websocketCategory => Some(context.actorOf(Props(new CamelWebsocketWriter(env.websocketBL, writer))))
      case _ => None //TODO gestire writer inaspettato
    }
  }
}

class CamelKafkaWriter(topicBL: TopicBL, writer: WriterModel) extends Producer {

  //TODO: Configurazione completa?
  val kafkaTopic: TopicModel = topicBL.getByName(writer.endpointName.get).get
  val topicSchema = kafkaTopic.getJsonSchema
  val topicDataType = kafkaTopic.topicDataType


  override def endpointUri: String = getKafkaUri(writer.name)

  def getKafkaUri(topic: String) = {
    val config = ConfigManager.getKafkaConfig
    val kafkaConnections = config.connections.mkString(",") // Why the "," https://github.com/apache/camel/blob/master/components/camel-kafka/src/test/java/org/apache/camel/component/kafka/KafkaComponentTest.java
    val zookeeperConnections = config.zookeeperConnections.getZookeeperConnection()

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

//TODO Migrate to the elastic consumer rt plugin
/*
class CamelElasticWriter(indexBL: IndexBL, writer: WriterModel) extends Producer {
  val indexConfigOpt: Option[IndexModel] = indexBL.getById(writer.id.getValue.toHexString)
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
    if (??[Boolean](WaspSystem.elasticAdminActor, CheckOrCreateIndex(indexName, index.name, index.dataType, index.getJsonSchema))) {
      //
    }
    val clusterName = ConfigManager.getElasticConfig.cluster_name
    //TODO se ci sono più connections assegnare così 'ips' va bene?
    s"elasticsearch://$clusterName?operation=INDEX&indexName=${indexName}&indexType=${index.dataType}&ip=${ips}"
  }

  override def transformOutgoingMessage(msg: Any): Any = {
    msg match {
      case m: String => {
        m
      }
      case camel: CamelMessage => {
        camel
      }
        //TODO
      case default: Any => ???
    }

  }

}
*/

class CamelWebsocketWriter(websocketBL: WebsocketBL, writer: WriterModel) extends Producer {

  val webSocketConfigOpt: Option[WebsocketModel] = websocketBL.getByName(writer.endpointName.get)
  if (webSocketConfigOpt.isEmpty) {
    //TODO eccezione? fallisce l'attore?
  }

  override def endpointUri: String = getHttpUri

  override def postStop() = {
    //PROBLEMA: http://stackoverflow.com/questions/33152976/how-to-stop-the-websocketcomponent-in-apache-camel?noredirect=1
    this.camelContext.getComponent("websocket", classOf[WebsocketComponent]).doStop()
    this.camelContext.removeComponent("websocket")
  }

  def getHttpUri = {
    def getOptions(websocketConfig: BsonDocument): String = {
      websocketConfig.entrySet().asScala.map( x => x.getKey + "=" + x.getValue.asString().getValue).mkString("&")
    }

    val rootUri = s"websocket://${webSocketConfigOpt.get.host}:${webSocketConfigOpt.get.port}/${webSocketConfigOpt.get.resourceName}"
    val finalUri = if (webSocketConfigOpt.get.options.isDefined) {
        rootUri + s"?${getOptions(webSocketConfigOpt.get.options.get)}"
    } else {
      rootUri
    }
    finalUri
  }
}
