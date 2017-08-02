package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.pattern.gracefulStop
import akka.routing.BalancingPool
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem, timeout}
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages.AddRemoteProducer
import it.agilelab.bigdata.wasp.core.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.core.{WaspMessage, WaspSystem}
import it.agilelab.bigdata.wasp.producers.remote._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case object StartProducer extends WaspMessage

case object StopProducer extends WaspMessage

abstract class ProducerMasterGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String) extends ClusterAwareNodeGuardian {
  val logger = WaspLogger(this.getClass.getName)
  
  val name: String
  
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

  override def uninitialized: Actor.Receive = super.uninitialized orElse guardianUnitialized

  override def initialized: Actor.Receive = {
    case StartProducer =>
      logger.info("startProducer")
      sender() ! true

    case StopProducer =>
      logger.info("StopProducer")
      stopChildActors
      sender() ! true
  }

  def guardianUnitialized: Actor.Receive = {
    case StartProducer =>
      initialize
      sender() ! true

    case StopProducer =>
      sender() ! true
  }

  def startChildActors()


  def stopChildActors() = {

    //Stop all actors bound to this guardian and the guardian itself
    logger.info(s"Stopping actors bound to ${this.getClass.getName} ...")

    val globalStatus = Future.traverse(context.children)(gracefulStop(_, timeout.duration))

    globalStatus map { res =>
      if (res reduceLeft (_ && _)) {

        logger.info(s"Graceful shutdown completed.")
        env.producerBL.setIsActive(producer, isActive = false)

        logger.info(s"Node is transitioning from 'initialized' to 'uninitialized'")
        kafka_router ! PoisonPill

        context become guardianUnitialized

      } else {
        logger.error(s"Something went wrong! Unable to shutdown all nodes")
      }
    }
  }

  override def initialize(): Unit = {

    val producerFuture = env.producerBL.getById(producerId)

    val kafkaConfig = ConfigManager.getKafkaConfig

    producerFuture map {
      p => {
        if (p.isDefined) {
          producer = p.get
          if (producer.hasOutput) {
            val topicFuture = env.producerBL.getTopic(topicBL = env.topicBL, producer)
            topicFuture map {
              topic => {
                associatedTopic = topic
                logger.info(s"Topic found  $topic")
                if (??[Boolean](WaspSystem.getKafkaAdminActor, CheckOrCreateTopic(topic.get.name, topic.get.partitions, topic.get.replicas))) {
                  logger.info("Before run kafka_router")
                  router_name = s"kafka-ingestion-router-$name-${producer._id.get.stringify}-${System.currentTimeMillis()}"
                  kafka_router = actorSystem.actorOf(BalancingPool(5).props(Props(new KafkaPublisherActor(ConfigManager.getKafkaConfig))), router_name)
                  logger.info("After run kafka_router")
                  context become initialized
                  startChildActors()

                  env.producerBL.setIsActive(producer, isActive = true)
                } else {
                  logger.error("Error creating topic " + topic.get.name)
                }

              }
            }
          } else {
            logger.warn("This producer hasn't associated topic")
          }
        } else {
          logger.error("Unable to fecth producer")
        }
      }

    }

  }

}


