package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest, HttpResponse}
import akka.routing.BalancingPool
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem}
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.{RestProducerRequest, Start, Stop}
import scala.concurrent.Future
import spray.json._
import NifiRquestJsonProtocol._

class NifiProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String)
  extends Actor
    with Logging {

  val name: String = "NifiProducerGuardian"
  implicit val materializer = ActorMaterializer()(WaspSystem.actorSystem)

  var nifiProducerConf: Option[ProducerModel] = env.producerBL.getById(producerId)
  val configuration: Option[String] = getConfiguration(nifiProducerConf)
  val url = s"http://localhost:1080"

  // initialized in initialize()
  var producer: ProducerModel = _
  var associatedTopic: Option[TopicModel] = _
  var router_name: String = _
  var kafka_router: ActorRef = _ // TODO: Careful with kafka router dynamic name

  override def receive: Actor.Receive = {

    case Start =>
      logger.info(s"Producer $producerId starting at guardian $self")

      if(configuration.isDefined) {
        get(url, getRequest(configuration.get, "RUNNING"))
        sender() ! true
      }

    case Stop =>
      logger.info(s"Producer $producerId stopping")

      if(configuration.isDefined) {
        get(url, getRequest(configuration.get, "STOPPED"))
        sender() ! true
      }

    case RestProducerRequest =>
      logger.info(s"Producer $producerId: generic request")

      if(configuration.isDefined) {
        get(url, getRequest(configuration.get, "UPDATE"))
        sender() ! true
      }
  }

  def initialize(): Unit = {

    val producerOption = env.producerBL.getById(producerId)

    if (producerOption.isDefined) {
      producer = producerOption.get
      if (producer.hasOutput) {
        val topicOption = env.producerBL.getTopic(topicBL = env.topicBL, producer)
        associatedTopic = topicOption
        logger.info(s"Producer $producerId: topic found: $associatedTopic")
        if (??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topicOption.get.name, topicOption.get.partitions, topicOption.get.replicas))) {
          router_name = s"kafka-ingestion-router-$name-${producer._id.get.getValue.toHexString}-${System.currentTimeMillis()}"
          kafka_router = actorSystem.actorOf(BalancingPool(5).props(Props(new KafkaPublisherActor(ConfigManager.getKafkaConfig))), router_name)

          env.producerBL.setIsActive(producer, isActive = true)
        } else {
          logger.error(s"Producer $producerId: error creating topic " + topicOption.get.name)
        }
      }

    } else {
      logger.warn(s"Producer $producerId: this producer doesn't have an associated topic")
    }
  }

  // Get the content of the configuration field from MongoDB
  def getConfiguration(producerConf: Option[ProducerModel]): Option[String] = {
    if (!producerConf.isDefined) None
    val conf = producerConf.get.configuration
    logger.info(s"Configuration $conf")
    conf
  }

  // Get Future[HttpResponse] representing http-request
  def get(url: String, jsonReq: JsValue): Future[HttpResponse] = {
    val method = HttpMethods.PUT
    Http().singleRequest(
      HttpRequest(uri = url)
        .withMethod(HttpMethods.PUT)
        .withEntity(ContentTypes.`application/json`, jsonReq.toString())
    )
  }

  def getRequest(json: String, action: String): JsValue = {
    val conf = json.parseJson.convertTo[NifiRequest]
    NifiRequest(action, conf.id, conf.child, conf.data).toJson
  }
}