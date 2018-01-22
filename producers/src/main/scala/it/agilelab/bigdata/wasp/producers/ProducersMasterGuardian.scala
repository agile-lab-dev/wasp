package it.agilelab.bigdata.wasp.producers

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.{??, actorSystem, mediator}
import it.agilelab.bigdata.wasp.core.bl.{ConfigBL, ProducerBL, TopicBL}
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
		extends Actor
		with WaspConfiguration
		with Logging {

	// subscribe to producers topic using distributed publish subscribe
	mediator ! Subscribe(WaspSystem.producersPubSubTopic, self)
	
	// all producers, whether they are local or remote
	private def retrieveProducers: Map[String, ActorRef] = retrieveSystemAndLocalProducers ++ remoteProducersComponentActors

	// system and local producers, for local producers also creating the related actors if they are not already created
	private def retrieveSystemAndLocalProducers: Map[String, ActorRef] = {
		env.producerBL
			.getAll // grab all producers
			.filterNot(_.isRemote) // filter only local ones
			.map(producer => {
			val producerId = producer._id.get.getValue.toHexString
			if (producer.name == InternalLogProducerGuardian.name) { // logger producer is special
				producerId -> WaspSystem.loggerActor // do not instantiate, but get the already existing one from WaspSystem
			} else {
				if(localProducerComponentActors.contains(producerId))
					producerId -> localProducerComponentActors(producerId) // do not instantiate, but get the already existing one from localProducerComponentActors
				else {
					val producerClass = Class.forName(producer.className)
					val producerActor = actorSystem.actorOf(Props(producerClass, ConfigBL, producerId), producer.name)

					// add to component actor tracking map
					localProducerComponentActors += (producerId -> producerActor)
					producerId -> producerActor
				}
			}
		}).toMap
	}
	
	// tracking map for local producer components ( componentName -> ProducerModel )
	// local producers, which run in the same JVM as the MasterGuardian (those with isRemote = false)
	private val localProducerComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]

	// tracking map for remote producer components ( componentName -> ProducerModel )
	// remote producers, which run in a different JVM than the MasterGuardian (those with isRemote = true)
	private val remoteProducersComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]

	override def preStart(): Unit = {
		// non-system producers are deactivated on startup
		logger.info("Deactivating non-system producers...")
		setProducersActive(env.producerBL.getNonSystemProducers, isActive = false)
		logger.info("Deactivated non-system producers")

		// activate/deactivate system producers according to config on startup
		// TODO manage error in producer initialization
		if (waspConfig.systemProducersStart) {
			logger.info("Activating system producers...")

			env.producerBL.getSystemProducers foreach {
				producer => {
					logger.info("Activating system producer \"" + producer.name + "\"...")
					WaspSystem.masterGuardian ! StartProducer(producer.name) // masterGuardian uses producer.name instead of producer._id.get.getValue.toHexString
					logger.info("Activated system producer \"" + producer.name + "\"")
				}
			}

			logger.info("Activated system producers")
		} else {
			logger.info("Deactivating system producers...")
			setProducersActive(env.producerBL.getSystemProducers, isActive = false)
			logger.info("Deactivated system producers")
		}
	}

	private def setProducersActive(producers: Seq[ProducerModel], isActive: Boolean): Unit = {
		producers.foreach(producer => env.producerBL.setIsActive(producer, isActive))
	}

	override def receive: Actor.Receive = {
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
		if (remoteProducersComponentActors.isDefinedAt(producerId)) { // already added
			Left(s"Remote producer $producerId ($producerActor) not added; already present.")
		} else { // add to remote producers & start if needed
			remoteProducersComponentActors += producerId -> producerActor
			if (producerModel.isActive) {
				self ! StartProducer(producerId)
			}
			Right(s"Remote producer $producerId ($producerActor) added.")
		}
	}
	
	private def removeRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
		val producerId = producerModel._id.get.getValue.toHexString
		if (remoteProducersComponentActors.isDefinedAt(producerId)) { // found, remove
			val producerActor = remoteProducersComponentActors(producerId)
			remoteProducersComponentActors.remove(producerId)
			Right(s"Remote producer $producerId ($producerActor) removed.")
		} else { // not found
			Left(s"Remote producer $producerId not found; either it was never added or it has already been removed.")
		}
	}

	private def startProducer(producer: ProducerModel): Either[String, String] = {
		val producers = retrieveProducers
		if (producers.isDefinedAt(producer._id.get.getValue.toHexString)) {

			//env.producerBL.setIsActive(producer, true)	// managed internally (ProducerGuardian)
			??[Either[String, Unit]](producers(producer._id.get.getValue.toHexString), Start) match {
				case Right(_) =>
					val msg = s"Producer '${producer.name}' started"
					logger.info(msg)
					Right(msg)
				case Left(s) =>
					//env.producerBL.setIsActive(producer, false)	// managed internally (ProducerGuardian)
					val msg = s"Producer '${producer.name}' not started - Message from ProducerGuardian: ${s}"
					logger.error(msg)
					Left(msg)
			}
		} else {
			Left(s"Producer '${producer.name}' does not exist")
		}
	}

	private def stopProducer(producer: ProducerModel): Either[String, String] = {
		val producers = retrieveProducers
		if (producers.isDefinedAt(producer._id.get.getValue.toHexString)) {

			//env.producerBL.setIsActive(producer, false)	// managed internally (ProducerGuardian)
			??[Either[String, Unit]](producers(producer._id.get.getValue.toHexString), Stop) match {
				case Right(_) =>
					val msg = s"Producer '${producer.name}' stopped"
					logger.info(msg)
					Right(msg)
				case Left(s) =>
					//env.producerBL.setIsActive(producer, true)	// managed internally (ProducerGuardian)
					val msg = s"Producer '${producer.name}' not stopped - Message from ProducerGuardian: ${s}"
					logger.error(msg)
					Left(msg)
			}
		} else {
			Left(s"Producer '${producer.name}' does not exist")
		}
	}

	private def restProducerRequest(request: RestProducerRequest, producer: ProducerModel): Either[String, String] = {
		val producers = retrieveProducers
		if (producers.isDefinedAt(producer._id.get.getValue.toHexString)) {

			??[Either[String, Unit]](producers(producer._id.get.getValue.toHexString), RestRequest(request.httpMethod, request.data, request.mlModelId.getOrElse(""))) match {
				case Right(_) =>
					val msg = s"Producer '${producer.name}' request: ${request.data}"
					logger.info(msg)
					Right(msg)
				case Left(s) =>
					val msg = s"Producer '${producer.name}' request not successful - Message from ProducerGuardian: ${s}"
					logger.error(msg)
					Left(msg)
			}
		} else {
			Left(s"Producer '${producer.name}' does not exist")
		}
	}
}