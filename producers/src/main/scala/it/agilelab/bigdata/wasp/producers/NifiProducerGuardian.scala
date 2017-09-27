package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
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
import it.agilelab.bigdata.wasp.core.messages.{Start, Stop}
import spray.json._
import DefaultJsonProtocol._
import scala.concurrent.Future

class NifiProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String)
  extends Actor
    with Logging {

  val name: String = "NifiProducerGuardian"
  implicit val materializer = ActorMaterializer()(WaspSystem.actorSystem)

  var nifiProducerConf: Option[ProducerModel] = env.producerBL.getById(producerId)

  val processGroupId: Option[String] = getProcessGroupId(nifiProducerConf)
  val url = s"http://localhost:8084/nifi-api/flow/process-groups/c257f39a-015e-1000-c58b-ec0b29141625"

  // initialized in initialize()
  var producer: ProducerModel = _
  var associatedTopic: Option[TopicModel] = _
  var router_name: String = _
  var kafka_router: ActorRef = _ // TODO: Careful with kafka router dynamic name

  override def receive: Actor.Receive = {

    case Start =>
      logger.info(s"Producer $producerId starting at guardian $self")

      if(processGroupId.isDefined) {
        get(url, requestType(processGroupId.get, "RUNNING"))
        sender() ! true
      }

    case Stop =>
      logger.info(s"Producer $producerId stopping")

      if(processGroupId.isDefined) {
        get(url, requestType(processGroupId.get, "STOPPED"))
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

  // Get ProcessGroup ID in BsonString from id of the nifiProducerConf
  def getProcessGroupId(producerConf: Option[ProducerModel]): Option[String] = {
    if (!producerConf.isDefined) None
    producerConf.get.configuration
  }

  // Get Future[HttpResponse] representing http-request
  def get(url: String, jsonReq: JsValue): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(uri = url)
        .withMethod(HttpMethods.PUT)
        .withEntity(ContentTypes.`application/json`, jsonReq.toString())
    )
  }

  // Set request-type (RUNNING or STOPPED)
  def requestType(id: String, reqType: String): JsValue =
    Map("id" -> id, "state" -> reqType).toJson
}