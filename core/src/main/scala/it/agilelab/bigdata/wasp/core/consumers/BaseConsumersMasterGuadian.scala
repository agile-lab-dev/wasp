package it.agilelab.bigdata.wasp.core.consumers

import akka.actor.{Actor, ActorRef, Stash}
import it.agilelab.bigdata.wasp.repository.core.bl.PipegraphBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.models.{
  LegacyStreamingETLModel,
  PipegraphModel,
  ProcessingComponentModel,
  RTModel,
  StructuredStreamingETLModel
}

/** Base class for consumer master guardians. Provides skeleton for behaviour and helpers.
	*
	* @author Nicolò Bidotti
	*/
abstract class BaseConsumersMasterGuadian(env: { val pipegraphBL: PipegraphBL }) extends Actor with Stash with Logging {

  // type alias for pipegraph -> components map
  type PipegraphsToComponentsMap =
    Map[PipegraphModel, (Seq[StructuredStreamingETLModel])]

  // counter for ready components
  protected var numberOfReadyComponents = 0

  // getter for total number masterGuardianof components that should be running
  def getTargetNumberOfReadyComponents: Int

  // ActorRef to MasterGuardian returned by the last ask - cannot be replaced with a simple ActorRef or singleton proxy!
  protected var masterGuardian: ActorRef = _

  // actor lifecycle callbacks =========================================================================================

  override def preStart(): Unit = {
    // we start in uninitialized state
    context become uninitialized
  }

  // behaviours ========================================================================================================

  // standard receive
  // NOTE: THIS IS IMMEDIATELY SWITCHED TO uninitialized DURING preStart(), DO NOT USE!
  override def receive: Actor.Receive = uninitialized

  // behaviour when uninitialized
  def uninitialized: Receive = {
    case RestartConsumers =>
      // update MasterGuardian ActorRef
      masterGuardian = sender()

      beginStartup()
  }

  // behaviour while starting
  def starting: Receive = {
    case Right(s) =>
      val msg = s"ETL started - Message from ETLActor: ${s}"
      logger.info(msg)

      // register component actor
      registerComponentActor(sender())

      if (numberOfReadyComponents == getTargetNumberOfReadyComponents) {
        // all component actors registered; finish startup
        logger.info(
          s"All $numberOfReadyComponents consumer child actors have registered! Continuing startup sequence..."
        )
        finishStartup()
      } else {
        logger.info(
          s"Not all component actors have registered to the cluster (right now only $numberOfReadyComponents " +
            s"out of $getTargetNumberOfReadyComponents), waiting for more..."
        )
      }

    case Left(s) =>
      val msg = s"ETL not started - Message from ETLActor: ${s}"
      logger.error(msg)
      finishStartup(success = false, errorMsg = msg)

    case RestartConsumers =>
      logger.info(s"Stashing RestartConsumers from ${sender()}")
      stash()
  }

  // behavior once initialized
  def initialized: Receive = {
    case Left(s) =>
      val msg = s"ETL not stopped - Message from ETLActor: ${s}"
      logger.error(msg)
    // do nothing:  leave go stop() to Timeout

    case RestartConsumers =>
      // update MasterGuardian ActorRef
      masterGuardian = sender()

      // attempt stopping
      val stoppingSuccessful = stop()

      // only proceed with restart if we actually stopped
      if (stoppingSuccessful) {
        logger.info("Stopping successful, starting up again")
        beginStartup()
      } else {
        logger.error("Stopping unsuccessful, not starting up again")
      }
  }

  // methods implementing start/stop ===================================================================================

  protected def beginStartup(): Unit

  protected def registerComponentActor(componentActor: ActorRef): Unit = {
    logger.info(s"Component actor $componentActor registered")
    numberOfReadyComponents += 1
  }

  protected def finishStartup(success: Boolean = true, errorMsg: String = ""): Unit

  protected def stop(): Boolean

  // helper methods ====================================================================================================

  protected def getActivePipegraphsToComponentsMap: PipegraphsToComponentsMap = {
    val pipegraphs: Seq[PipegraphModel] = env.pipegraphBL.getActivePipegraphs()

    // extract components from active pipegraphs as a Map pipegraph -> components
    val pipegraphsToComponentsMap: PipegraphsToComponentsMap = pipegraphs map { pipegraph =>
      {
        // grab components
        val sseComponents = pipegraph.structuredStreamingComponents

        pipegraph -> (sseComponents)
      }
    } toMap

    pipegraphsToComponentsMap
  }
}

object BaseConsumersMasterGuadian {
  def generateUniqueComponentName(pipegraph: PipegraphModel, component: ProcessingComponentModel): String = {
    pipegraph.generateStandardPipegraphName + "_" + component.generateStandardProcessingComponentName + "_" + component.generateStandardWriterName
  }
}
