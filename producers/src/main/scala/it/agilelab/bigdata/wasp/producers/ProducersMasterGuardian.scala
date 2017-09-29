package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem, mediator}
import it.agilelab.bigdata.wasp.core.bl.{ConfigBL, ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages._
import it.agilelab.bigdata.wasp.core.models.ProducerModel
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration

import scala.collection.mutable

/**
	* Master guardian for WASP producers.
	*
	* @author Nicolò Bidotti
	*/
class ProducersMasterGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL})
	  extends ClusterAwareNodeGuardian
		with WaspConfiguration
		with Logging {
	// subscribe to producers topic using distributed publish subscribe
	mediator ! Subscribe(WaspSystem.producersPubSubTopic, self)
	
	// all producers, whether they are local or remote
	def producers: Map[String, ActorRef] = localProducers ++ remoteProducers
	
	// local producers, which run in the same JVM as the MasterGuardian (those with isRemote = false)
	val localProducers: Map[String, ActorRef] = {
		env.producerBL
			.getAll // grab all producers
			.filterNot(_.isRemote) // filter only local ones
			.map(producer => {
			val producerId = producer._id.get.getValue.toHexString
			if (producer.name == InternalLogProducerGuardian.name) { // logger producer is special
				producerId -> WaspSystem.loggerActor // do not instantiate, but get the already existing one from WaspSystem
			} else {
				val producerClass = Class.forName(producer.className)
				val producerActor = actorSystem.actorOf(Props(producerClass, ConfigBL, producerId), producer.name)
				producerId -> producerActor
			}
		}).toMap
	}
	
	// remote producers, which run in a different JVM than the MasterGuardian (those with isRemote = true)
	val remoteProducers: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
	
	// on startup non-system producers are deactivated
	logger.info("Deactivating non-system producers...")
	setProducersActive(env.producerBL.getNonSystemProducers, isActive = false)
	logger.info("Deactivated non-system producers")
	
	// activate/deactivate system producers according to config on startup
	// TODO manage error in pipegraph initialization
	if (waspConfig.systemProducersStart) {
		logger.info("Activating system producers...")
		
		env.producerBL.getSystemProducers foreach {
			producer => {
				logger.info("Activating system producer \"" + producer.name + "\"...")
				WaspSystem.masterGuardian ! StartProducer(producer._id.get.getValue.toHexString)
				logger.info("Activated system producer \"" + producer.name + "\"")
			}
		}
		
		logger.info("Activated system producers")
	} else {
		logger.info("Deactivating system producers...")
		setProducersActive(env.producerBL.getSystemProducers, isActive = false)
		logger.info("Deactivated system producers")
	}
	
	
	private def setProducersActive(producers: Seq[ProducerModel], isActive: Boolean): Unit = {
		producers.foreach(producer => env.producerBL.setIsActive(producer, isActive))
	}
	
	// TODO try without sender parenthesis
	def initialized: Actor.Receive = {
		case message: AddRemoteProducer => call(message.remoteProducer, message, onProducer(message.id, addRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
		case message: RemoveRemoteProducer => call(message.remoteProducer, message, onProducer(message.id, removeRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
		case message: StartProducer => call(sender(), message, onProducer(message.id, startProducer))
		case message: StopProducer => call(sender(), message, onProducer(message.id, stopProducer))
    case message: RestProducerRequest => call(sender(), message, onProducer(message.id, restProducerRequest(message, _)))
	}
	
	private def call[T <: MasterGuardianMessage](sender: ActorRef, message: T, result: Either[String, String]): Unit = {
		logger.info(s"Call invocation: message: $message result: $result")
		sender ! result
	}
	
	private def onProducer(id: String, f: ProducerModel => Either[String, String]): Either[String, String] = {
		env.producerBL.getById(id) match {
			case None => Right("Producer not retrieved")
			case Some(producer) => f(producer)
		}
	}
	
	private def addRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
		val producerId = producerModel._id.get.getValue.toHexString
		if (remoteProducers.isDefinedAt(producerId)) { // already added
			Left(s"Remote producer $producerId ($producerActor) not added; already present.")
		} else { // add to remote producers & start if needed
			remoteProducers += producerId -> producerActor
			if (producerModel.isActive) {
				self ! StartProducer(producerId)
			}
			Right(s"Remote producer $producerId ($producerActor) added.")
		}
	}
	
	private def removeRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
		val producerId = producerModel._id.get.getValue.toHexString
		if (remoteProducers.isDefinedAt(producerId)) { // found, remove
			val producerActor = remoteProducers(producerId)
			remoteProducers.remove(producerId)
			Right(s"Remote producer $producerId ($producerActor) removed.")
		} else { // not found
			Left(s"Remote producer $producerId not found; either it was never added or it has already been removed.")
		}
	}
	
	private def startProducer(producer: ProducerModel): Either[String, String] = {
		// initialise producer actor if not already present
		if (producers.isDefinedAt(producer._id.get.getValue.toHexString)) {
			if (! ??[Boolean](producers(producer._id.get.getValue.toHexString), Start)) {
				Left(s"Producer '${producer.name}' not started")
			} else {
				Right(s"Producer '${producer.name}' started")
			}
		} else {
			Left(s"Producer '${producer.name}' does not exist")
		}
		
	}
	
	private def stopProducer(producer: ProducerModel): Either[String, String] = {
		if (!producers.isDefinedAt(producer._id.get.getValue.toHexString)) {
			Left("Producer '" + producer.name + "' not initialized")
		} else if (! ??[Boolean](producers(producer._id.get.getValue.toHexString), Stop)) {
			Left("Producer '" + producer.name + "' not stopped")
		} else {
			Right("Producer '" + producer.name + "' stopped")
		}
	}

  private def restProducerRequest(request: RestProducerRequest, producer: ProducerModel): Either[String, String] = {
    if (!producers.isDefinedAt(producer._id.get.getValue.toHexString)) {
      Left("Producer '" + producer.name + "' not initialized")
    } else if (! ??[Boolean](producers(producer._id.get.getValue.toHexString), Stop)) {
      Left("Producer '" + producer.name + "' not stopped")
    } else {
      Right("Producer '" + producer.name + "' stopped")
    }
  }
}