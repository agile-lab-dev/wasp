package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.gracefulStop
import akka.routing.BalancingPool
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem, synchronousActorCallTimeout}
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.{ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.{Start, Stop}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Base class for a WASP producer. A ProducerGuardian represents a producer and manages the lifecycle of the child
  * ProducerActors that actually produce the data.
  */
abstract class ProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerId: String) extends ClusterAwareNodeGuardian {
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
    case Start =>
      logger.info(s"Producer $producerId starting at guardian $self")
      sender() ! true

    case Stop =>
      logger.info(s"Producer $producerId stopping")
      stopChildActors()
      sender() ! true
  }

  def guardianUnitialized: Actor.Receive = {
    case Start =>
      initialize()
      sender() ! true

    case Stop =>
      sender() ! true
  }

  def startChildActors()


  def stopChildActors(): Future[Unit] = {

    //Stop all actors bound to this guardian and the guardian itself
    logger.info(s"Producer $producerId: stopping actors bound to $self...")

    val globalStatus = Future.traverse(context.children)(gracefulStop(_, synchronousActorCallTimeout.duration))

    globalStatus map { res =>
      if (res reduceLeft (_ && _)) {

        logger.info(s"Producer $producerId: graceful shutdown completed.")
        env.producerBL.setIsActive(producer, isActive = false)

        logger.info(s"Producer $producerId: transitioning from 'initialized' to 'uninitialized'")
        kafka_router ! PoisonPill

        context become guardianUnitialized

      } else {
        logger.error(s"Producer $producerId: something went wrong! Unable to shutdown all nodes")
      }
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
          router_name = s"kafka-ingestion-router-$name-${producer._id.get.asString()}-${System.currentTimeMillis()}"
          kafka_router = actorSystem.actorOf(BalancingPool(5).props(Props(new KafkaPublisherActor(ConfigManager.getKafkaConfig))), router_name)
          context become initialized
          startChildActors()

          env.producerBL.setIsActive(producer, isActive = true)
        } else {
          logger.error(s"Producer $producerId: error creating topic " + topicOption.get.name)
        }

      }

    } else {
      logger.warn(s"Producer $producerId: this producer doesn't have an associated topic")
    }
  }

}


