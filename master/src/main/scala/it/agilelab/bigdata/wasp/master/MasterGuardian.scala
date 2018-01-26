package it.agilelab.bigdata.wasp.master

import java.util.Calendar

import akka.actor.{Actor, ActorRef, actorRef2Scala}
import akka.pattern.ask
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages._
import it.agilelab.bigdata.wasp.core.models.{BatchJobModel, PipegraphModel, ProducerModel}
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspConfiguration}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.TimeoutException
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

      WaspSystem.??[Either[String, String]](masterGuardian, RestartPipegraphs) match {
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
    // non-system pipegraphs and associated consumers are deactivated on startup
    logger.info("Deactivating non-system pipegraphs...")
    setPipegraphsActive(env.pipegraphBL.getNonSystemPipegraphs, isActive = false)
    logger.info("Deactivated non-system pipegraphs")

    // activate/deactivate system pipegraphs and associated consumers according to config on startup
    // TODO manage error in pipegraph initialization
    if (waspConfig.systemPipegraphsStart) {
      logger.info("Activating system pipegraphs...")

      // TODO use single-restart method; setActive + dummy StartPipegraph?
      env.pipegraphBL.getSystemPipegraphs foreach {
        pipegraph => {
          logger.info(s"Scheduling startup of system pipegraph '${pipegraph.name}'")
          self.actorRef ! StartPipegraph(pipegraph.name)
        }
      }

      logger.info("Scheduled the startup of all system pipegraphs")
    } else {
      logger.info("Deactivating system pipegraphs...")
      setPipegraphsActive(env.pipegraphBL.getSystemPipegraphs, isActive = false)
      logger.info("Deactivated all system pipegraphs")
    }

    // start batch schedulers
    logger.info("Starting batch schedulers...")
    batchMasterGuardian ! StartSchedulersMessage()
  }

  override def receive: Actor.Receive = {
    //case message: RemovePipegraph => call(message, onPipegraph(message.id, removePipegraph))
    case message: StartPipegraph => call(sender(), message, onPipegraph(message.id, startPipegraph))
    case message: StopPipegraph => call(sender(), message, onPipegraph(message.id, stopPipegraph))
    case RestartPipegraphs => call(sender(), RestartPipegraphs, onRestartPipegraphs())
    case message: AddRemoteProducer => call(message.remoteProducer, message, onProducer(message.id, addRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
    case message: RemoveRemoteProducer => call(message.remoteProducer, message, onProducer(message.id, removeRemoteProducer(message.remoteProducer, _))) // do not use sender() for actor ref: https://github.com/akka/akka/issues/17977
    case message: StartProducer => call(sender(), message, onProducer(message.id, startProducer))
    case message: StopProducer => call(sender(), message, onProducer(message.id, stopProducer))
    case message: RestProducerRequest => call(sender(), message, onProducer(message.id, restProducerRequest(message, _)))
    case message: StartETL => call(sender(), message, onEtl(message.id, message.etlName, startEtl))
    case message: StopETL => call(sender(), message, onEtl(message.id, message.etlName, stopEtl))
    case message: StartBatchJob => call(sender(), message, onBatchJob(message.id, startBatchJob))
    case message: StartPendingBatchJobs => call(sender(), message, startPendingBatchJobs())
    case message: BatchJobProcessedMessage => //TODO gestione batchJob finito?
    //case message: Any => logger.error("unknown message: " + message)
  }

  private def setPipegraphsActive(pipegraphs: Seq[PipegraphModel], isActive: Boolean): Unit = {
    pipegraphs.foreach(pipegraph => env.pipegraphBL.setIsActive(pipegraph, isActive))
  }

  private def call[T <: MasterGuardianMessage](sender: ActorRef, message: T, result: Either[String, String]): Unit = {
    logger.info(s"Call invocation: message: $message result: $result")
    sender ! result
  }

  private def onPipegraph(name: String, f: PipegraphModel => Either[String, String]): Either[String, String] = {
    env.pipegraphBL.getByName(name) match {
      case None => Left("Pipegraph not retrieved")
      case Some(pipegraph) => f(pipegraph)
    }
  }

  private def onRestartPipegraphs(): Either[String, String] = {
    sparkConsumersMasterGuardian ! RestartConsumers
    rtConsumersMasterGuardian ! RestartConsumers
    Right("Pipegraphs restart started.")
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

  private def onBatchJob(name: String, f: BatchJobModel => Either[String, String]): Either[String, String] = {
    env.batchJobBL.getByName(name) match {
      case None => Left("BatchJob not retrieved")
      case Some(batchJob) => f(batchJob)
    }
  }

  private def manageThrowable(message: String, throwable: Throwable): String = {
    var result = message
    logger.error(message, throwable)

    if (throwable.getMessage != null && !throwable.getMessage.isEmpty)
      result = result + " Cause: " + throwable.getMessage

    result
  }

  private def cleanPipegraph(message: RemovePipegraph): Unit = {
    //sender() ! true
  }

  private def startPipegraph(pipegraph: PipegraphModel): Either[String, String] = {
    setActiveAndRestart(pipegraph, active = true)
  }

  private def stopPipegraph(pipegraph: PipegraphModel): Either[String, String] = {
    setActiveAndRestart(pipegraph, active = false)
  }

  private def setActiveAndRestart(pipegraph: PipegraphModel, active: Boolean): Either[String, String] = {

    // TODO revise this behaviour (pre-global-modification)
    /* modify the isActive flag
        startPipegraph -> pipegraph and all components isActive flags = true
        stopPipegraph -> pipegraph and all components isActive flags = false
     */
    env.pipegraphBL.setIsActive(pipegraph, isActive = active)

    var msgAdditional = ""
    // ask the guardians to restart only if the pipegraph has components that involve them
    val resSpark = if (pipegraph.hasSparkComponents) {
      try {
        ??[Either[String, String]](sparkConsumersMasterGuardian, RestartConsumers, Some(generalTimeout.duration)) match {
          case Right(_) =>
            true
          case Left(s) =>
            msgAdditional = s" - Message from SparkConsumerMasterGuardian: ${s}"
            false
        }
      } catch {
        case e: TimeoutException => {
          msgAdditional = " - Timeout from SparkConsumerMasterGuardian"
          false
        }
        case e: Exception => {
          msgAdditional = s" - Exception from RtConsumerMasterGuardian: ${e.getMessage}"
          false
        }
      }
    } else { // no spark components in pipegraph => true by default
      true
    }
    val resRt = if (pipegraph.hasRtComponents) {
      try {
        ??[Either[String, String]](rtConsumersMasterGuardian, RestartConsumers, Some(generalTimeout.duration)) match {
          case Right(_) =>
            true
          case Left(s) =>
            msgAdditional = s" - Message from RtConsumerMasterGuardian: ${s}"
            false
        }
      } catch {
        case e: TimeoutException => {
          msgAdditional = " - Timeout from RtConsumerMasterGuardian"
          false
        }
        case e: Exception => {
          msgAdditional = s" - Exception from RtConsumerMasterGuardian: ${e.getMessage}"
          false
        }
      }
    } else { // no rt components in pipegraph => true by default
      true
    }

    if (resSpark && resRt) { // everything ok
      val msg = s"Pipegraph '${pipegraph.name}'" + (if (active) "started" else "stopped")
      Right(msg + msgAdditional)
    } else { // something broke
      /** TODO: possible inconsistent state with partially started/stopped pipegraphs - see ISC-204
        * Choose a recovery strategy (es. stop/start all components, restart/restop components not started/stopped, ...)
        * */

      // TODO revise this behaviour (post-global-modification)
      /* undo isActive flag modification
          startPipegraph -> pipegraph and all components isActive flags = false
          stopPipegraph -> pipegraph and all components isActive flags = true
       */
      env.pipegraphBL.setIsActive(pipegraph, isActive = !active)

      val msg = s"Pipegraph '${pipegraph.name}' not " + (if (active) "started" else "stopped")
      Left(msg + msgAdditional)
    }
  }

  // TODO
  private def startEtl(pipegraph: PipegraphModel, etlName: String): Either[String, String] = {
    Left(s"Pipegraph '${pipegraph.name} - ETL '$etlName' not started [NOT IMPLEMENTED]")
  }

  // TODO
  private def stopEtl(pipegraph: PipegraphModel, etlName: String): Either[String, String] = {
    Left(s"Pipegraph '${pipegraph.name} - ETL '$etlName' not stopped [NOT IMPLEMENTED]")
  }

  private def addRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
    val producerId = producerModel._id.get.getValue.toHexString
    ??[Either[String, String]](producersMasterGuardian, AddRemoteProducer(producerId, producerActor))
  }

  private def removeRemoteProducer(producerActor: ActorRef, producerModel: ProducerModel): Either[String, String] = {
    val producerId = producerModel._id.get.getValue.toHexString
    ??[Either[String, String]](producersMasterGuardian, RemoveRemoteProducer(producerId, producerActor))
  }

  private def startProducer(producer: ProducerModel): Either[String, String] = {
    val producerId = producer._id.get.getValue.toHexString
    ??[Either[String, String]](producersMasterGuardian, StartProducer(producerId))
  }

  private def stopProducer(producer: ProducerModel): Either[String, String] = {
    val producerId = producer._id.get.getValue.toHexString
    ??[Either[String, String]](producersMasterGuardian, StopProducer(producerId))
  }

  private def restProducerRequest(request: RestProducerRequest, producer: ProducerModel): Either[String, String] = {
    val producerId = producer._id.get.getValue.toHexString
    ??[Either[String, String]](producersMasterGuardian, request.copy(id = producerId))
  }

  private def startBatchJob(batchJob: BatchJobModel): Either[String, String] = {
    logger.info(s"Starting batch job '${batchJob.name}'")
    val jobRes = ??[BatchJobResult](batchMasterGuardian, StartBatchJobMessage(batchJob._id.get.getValue.toHexString), Some(generalTimeout.duration))
    if (jobRes.result) {
      Right(s"Batch job '${batchJob.name}' accepted (queued or processing)")
    } else {
      Left(s"Batch job '${batchJob.name}' not accepted")
    }
  }

  private def startPendingBatchJobs(): Either[String, String] = {
    logger.info("Scheduling check of batch jobs bucket")
    batchMasterGuardian ! CheckJobsBucketMessage()
    Right("Scheduled the check of batch jobs bucket")
  }
}