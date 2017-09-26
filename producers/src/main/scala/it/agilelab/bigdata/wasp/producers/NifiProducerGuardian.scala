package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.routing.BalancingPool
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem}
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.{Start, Stop}
import org.mongodb.scala.bson.BsonString
import spray.json._

import scala.concurrent.Future

abstract class NifiProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String)
  extends ClusterAwareNodeGuardian
    with Logging {

  val name: String
  var nifiProducerConf: Option[ProducerModel] = env.producerBL.getById(producerId)
  val processGroupId: Option[BsonString] = getProcessGroupId("ID") // TODO: Set correct ProcessGroup ID
  val url = s"server02.cluster01.atscom.it:7070/nifi" // TODO: Set correct nifi-server address

  // initialized in initialize()
  var producer: ProducerModel = _
  var associatedTopic: Option[TopicModel] = _
  var router_name: String = _
  var kafka_router: ActorRef = _ // TODO: Careful with kafka router dynamic name

  override def postStop(): Unit = {
    super.postStop()
    kafka_router ! PoisonPill
  }

  override def preStart(): Unit = {
    super.preStart()
  }

  override def uninitialized: Actor.Receive = super.uninitialized

  override def initialized: Actor.Receive = {

    case Start =>
      logger.info(s"Producer $producerId starting at guardian $self")

      if(processGroupId.isDefined) {
        val responseFuture = get(url, requestType(processGroupId.toString, "RUNNING"))
        sender() ! true
      }

    case Stop =>
      logger.info(s"Producer $producerId stopping")

      if(processGroupId.isDefined) {
        val responseFuture = get(url, requestType(processGroupId.toString, "STOPPED"))
        sender() ! true
      }
  }

  override def initialize(): Unit = {

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
          context become initialized
          //startChildActors()

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
  def getProcessGroupId(id: String): Option[BsonString] = {
    nifiProducerConf
      .flatMap(
        elem => elem.configuration
          .map(conf => conf.getString(id)))
  }

  // Get Future[HttpResponse] representing http-request
  def get(url: String, jsonReq: Unit): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(uri = url)
        .withHeaders(RawHeader("Accept", "application/json"))
        .withHeaders(RawHeader("Content-Type", "application/json"))
        .withEntity(jsonReq.toString)
    )
  }

  // Set request-type (RUNNING or STOPPED)
  def requestType(id: String, reqType: String): Unit = {
    NifiStatus(id, reqType, "").toJson
  }
}

case class NifiStatus(id: String, state: String, components: String)