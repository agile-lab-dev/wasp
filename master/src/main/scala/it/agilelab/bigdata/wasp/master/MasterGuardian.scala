package it.agilelab.bigdata.wasp.master

import java.util.Calendar

import akka.actor.{Actor, ActorRef, actorRef2Scala}
import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.PipegraphMessages.StartSystemPipegraphs
import it.agilelab.bigdata.wasp.core.messages.{PipegraphMessages, _}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspConfiguration}
import it.agilelab.bigdata.wasp.models.{BatchJobModel, PipegraphModel, ProducerModel}
import spray.json.{JsObject, JsString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, HOURS, MILLISECONDS}

object MasterGuardian
  extends WaspConfiguration
    with Logging {

  // at midnight, restart all active pipelines (this causes new timed indices creation and consumers redirection on new indices)
  if (ConfigManager.getWaspConfig.indexRollover) {
    val timeToFirst = ceilDayTime(System.currentTimeMillis) - System.currentTimeMillis
    val initialDelay = Duration(timeToFirst, MILLISECONDS)
    val interval = Duration(24, HOURS)
    logger.info(f"Index rollover is enabled: scheduling index rollover ${initialDelay.toUnit(HOURS)}%4.2f hours from now and then every $interval")
    actorSystem.scheduler.schedule(initialDelay, interval) {

      ??[Either[String, String]](masterGuardian, RestartPipegraphs) match {
        case Right(s) => logger.info(s"RestartPipegraphs: $s")
        case Left(s) => logger.error(s"Failure during the pipegraphs restarting: $s")
      }
    }
  } else {
    logger.info("Index rollover is disabled.")
  }

  private def ceilDayTime(time: Long): Long = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DATE, 1)
    cal.getTime.getTime
  }
}

class MasterGuardian(env: {
                      val producerBL: ProducerBL
                      val pipegraphBL: PipegraphBL
                      val batchJobBL: BatchJobBL
                      val batchSchedulerBL: BatchSchedulersBL
                     },
                     classLoader: Option[ClassLoader] = None)
  extends Actor
    with Logging {

  import MasterGuardian._


  override def preStart(): Unit = {
    if (waspConfig.systemPipegraphsStart) {
      logger.info("Activating system pipegraphs...")

      sparkConsumersStreamingMasterGuardian ! StartSystemPipegraphs
    }
  }


  override def receive: Actor.Receive = {
    case RestartPipegraphs => call(sender(), RestartPipegraphs, restartPipegraphs())
    case message: AddRemoteProducer => call(message.remoteProducer, message, onProducer(message.name, addRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
    case message: RemoveRemoteProducer => call(message.remoteProducer, message, onProducer(message.name, removeRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
    case message: StartProducer => call(sender(), message, onProducer(message.name, startProducer))
    case message: StopProducer => call(sender(), message, onProducer(message.name, stopProducer))
    case message: RestProducerRequest => call(sender(), message, onProducer(message.name, restProducerRequest(message, _)))
    case message: StartETL => call(sender(), message, onEtl(message.name, message.etlName, startEtl))
    case message: StopETL => call(sender(), message, onEtl(message.name, message.etlName, stopEtl))
    case message: StartBatchJob => call(sender(), message, onBatchJob(message.name, message.restConfig, startBatchJob))
    case message: StartPipegraph => call(sender(), message, onPipegraph(message.name, startPipegraph))
    case message: StopPipegraph => call(sender(), message, onPipegraph(message.name, stopPipegraph))
    //case message: Any => logger.error("unknown message: " + message)
  }

  private def call[T <: MasterGuardianMessage](sender: ActorRef, message: T, result: Either[String, String]): Unit = {
    logger.info(s"Call invocation: message: $message result: $result")
    sender ! result
  }



  private def onProducer(name: String, f: ProducerModel => Either[String, String]): Either[String, String] = {
    env.producerBL.getByName(name) match {
      case None => Left("Producer not retrieved")
      case Some(producer) => f(producer)
    }
  }

  private def onEtl(pipegraphName: String, etlName: String, f: (PipegraphModel, String) => Either[String, String]): Either[String, String] = {
    env.pipegraphBL.getByName(pipegraphName) match {
      case None => Left("ETL not retrieved")
      case Some(pipegraph) => f(pipegraph, etlName)
    }
  }

  private def onPipegraph(name: String, f: PipegraphModel => Either[String, String]): Either[String, String] = {
    env.pipegraphBL.getByName(name) match {
      case None => Left("Pipegraph not retrieved")
      case Some(pipegraph) => f(pipegraph)
    }
  }

  private def onBatchJob(name: String, restConfig: Config, f: (BatchJobModel, Config) => Either[String, String]): Either[String, String] = {
    env.batchJobBL.getByName(name) match {
      case None => Left("BatchJob not retrieved")
      case Some(batchJob) => f(batchJob, restConfig)
    }
  }

  // TODO revise
  private def restartPipegraphs(): Either[String, String] = {
    sparkConsumersStreamingMasterGuardian ! RestartConsumers
    Right("Pipegraphs restart started.")
  }


  // TODO implement
  private def startEtl(pipegraph: PipegraphModel, etlName: String): Either[String, String] = {
    Left(s"Pipegraph '${pipegraph.name} - ETL '$etlName' not started [NOT IMPLEMENTED]")
  }

  // TODO implement
  private def stopEtl(pipegraph: PipegraphModel, etlName: String): Either[String, String] = {
    Left(s"Pipegraph '${pipegraph.name} - ETL '$etlName' not stopped [NOT IMPLEMENTED]")
  }

  private def addRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
    ??[Either[String, String]](producersMasterGuardian, AddRemoteProducer(producerModel.name, producerActor))
  }

  private def removeRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
    ??[Either[String, String]](producersMasterGuardian, RemoveRemoteProducer(producerModel.name, producerActor))
  }

  private def startProducer(producer: ProducerModel): Either[String, String] = {
    ??[Either[String, String]](producersMasterGuardian, StartProducer(producer.name))
  }

  private def stopProducer(producer: ProducerModel): Either[String, String] = {
    ??[Either[String, String]](producersMasterGuardian, StopProducer(producer.name))
  }

  private def restProducerRequest(request: RestProducerRequest, producer: ProducerModel): Either[String, String] = {
    ??[Either[String, String]](producersMasterGuardian, request.copy(name = producer.name))
  }

  private def startBatchJob(batchJob: BatchJobModel, restConfig: Config): Either[String, String] = {
    logger.info(s"Starting batch job '${batchJob.name}'")
    ??[BatchMessages.StartBatchJobResult](sparkConsumersBatchMasterGuardian, BatchMessages.StartBatchJob(batchJob.name, restConfig)) match {
      case BatchMessages.StartBatchJobResultSuccess(name, instanceName) =>
        Right(
          JsObject(
            "startResult" -> JsString(s"Batch job '$name' start accepted'"),
            "instance" -> JsString(s"$instanceName")
          ).toString
        )
      case BatchMessages.StartBatchJobResultFailure(name, error) => Left(s"Batch job '$name' start not accepted due to [$error]")
    }
  }

  private def startPipegraph(pipegraph: PipegraphModel): Either[String, String] = {
    logger.info(s"Starting pipegraph '${pipegraph.name}'")
    ??[PipegraphMessages.StartPipegraphResult](sparkConsumersStreamingMasterGuardian, PipegraphMessages.StartPipegraph(pipegraph.name)) match {
      case PipegraphMessages.PipegraphStarted(name, instanceName) =>
        Right(
          JsObject(
            "startResult" -> JsString(s"Pipegraph '$name' start accepted'"),
            "instance" -> JsString(s"$instanceName")
          ).toString
        )
      case PipegraphMessages.PipegraphNotStarted(name, error) => Left(s"Pipegraph '$name' start not accepted due to [$error]")
    }
  }

  private def stopPipegraph(pipegraph: PipegraphModel): Either[String, String] = {
    logger.info(s"Stopping pipegraph '${pipegraph.name}'")
    ??[PipegraphMessages.StopPipegraphResult](sparkConsumersStreamingMasterGuardian, PipegraphMessages.StopPipegraph
    (pipegraph.name)) match {
      case PipegraphMessages.PipegraphStopped(name) => Right(s"Pipegraph '$name' stop accepted")
      case PipegraphMessages.PipegraphNotStopped(name, error) => Left(s"Pipegraph '$name' stop not accepted due to [$error]")
    }
  }
}