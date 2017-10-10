package it.agilelab.bigdata.wasp.consumers.rt

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.core.models.RTModel
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


class RtConsumersMasterGuardian(env: {
                                       val pipegraphBL: PipegraphBL
                                       val topicBL: TopicBL
                                       val indexBL: IndexBL
                                       val websocketBL: WebsocketBL
                                     })
    extends ClusterAwareNodeGuardian
    with Stash
    with Logging
    with WaspConfiguration {
  
  // initial state is uninitialized
  context become uninitialized

  // internal state
  private var lastRestartMasterRef: ActorRef = _ // last master actor ref, needed for ask pattern
  private var numActiveRtComponents = 0 // number of currently active rt components (i.e. running)
  private var targetNumActiveRtComponents = 0 // target number of active rt components (i.e. should be running)

  // available states ==================================================================================================
  
  override def uninitialized: Actor.Receive = {
    case RestartConsumers =>
      lastRestartMasterRef = sender()
      startGuardian()
  }

  def starting: Actor.Receive = {
    case OutputStreamInitialized =>
      logger.info(s"RT component ${sender()} confirmed it is running")
      numActiveRtComponents += 1
      initialize()
    case RestartConsumers =>
      logger.info(s"Stashing restart ...")
      stash()
  }

  /** This node guardian's customer behavior once initialized. */
  override def initialized: Actor.Receive = {
    case RestartConsumers =>
      lastRestartMasterRef = sender()
      val stopSuccessful = stopGuardian()
      if (stopSuccessful) { // restart only if stopping was successful
        startGuardian()
      }
  }
  
  // overriden ClusterAwareNodeGuardian methods ========================================================================
  
  // TODO do we really need joinSeedNodes here? we are we trying to join ourselves???
  override def preStart(): Unit = {
    super.preStart()
    //TODO:capire joinseednodes
    cluster.joinSeedNodes(Vector(cluster.selfAddress))
  }
  
  // TODO should we really be calling this multiple times? maybe it should be "attemptInitialization()"?
  override def initialize(): Unit = {
    super.initialize()
  
    logger.info(s"Currently $numActiveRtComponents out of $targetNumActiveRtComponents active RT components are " +
                 "confirmed as running")
    
    if (numActiveRtComponents == targetNumActiveRtComponents) {
      logger.info("All active RT components are confirmed as running; entering initialized state")
      context become initialized
      logger.info("Sending success to master guardian")
      lastRestartMasterRef ! true
      logger.info(s"Startup sequence completed, all $targetNumActiveRtComponents RT components are running")
      logger.info("Unstashing queued messages...")
      unstashAll()
    } else {
      logger.info("Not all active RT components are confirmed as running; waiting for the remaining ones")
    }
  }

  // private methods ===================================================================================================

  private def stopGuardian(): Boolean = {
    logger.info(s"Stopping...")
    
    // stop all actors bound to this guardian and the guardian itself
    logger.info(s"Stopping child actors bound to this rt consumers master guardian $self")
    val generalTimeoutDuration = generalTimeout.duration
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, generalTimeoutDuration))
    val res = Await.result(globalStatus, generalTimeoutDuration)

    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
      numActiveRtComponents = 0
      true
    } else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all children")
      numActiveRtComponents = context.children.size
      logger.error(s"Found $numActiveRtComponents children still running")
      lastRestartMasterRef ! false
      false
    }
  }

  private def startGuardian(): Unit = {
    logger.info(s"Starting up...")
    
    // find all rt components marked as active, save how many they are in state
    val activeRtComponents = findActiveRtComponents
    targetNumActiveRtComponents = activeRtComponents.size
    
    if (activeRtComponents.isEmpty) {
      // no rts to start
      logger.info(s"Found $targetNumActiveRtComponents RT components marked as active; entering uninitialized state")
      context become uninitialized
      logger.info("Sending success to master guardian")
      lastRestartMasterRef ! true
      logger.info("Startup sequence completed, no RT components started")
    } else {
      // start rts
      logger.info(s"Found $targetNumActiveRtComponents RT components marked as active; entering starting state")
      context become starting
      logger.info("Starting RT components...")
      activeRtComponents foreach {
        rtComponent => {
          logger.info(s"Starting RT actor for component ${rtComponent.name}")
          val rtActor = context.actorOf(Props(new ConsumerRTActor(env, rtComponent, self)))
          rtActor ! StartRT
          logger.info(s"Started RT actor for component ${rtComponent.name}")
        }
      }
      logger.info(s"Started $targetNumActiveRtComponents RT components; waiting for confirmation that they are all running")
    }
  }

  private def findActiveRtComponents: Seq[RTModel] = {
    logger.info(s"Finding all active RT components...")
    
    val activePipegraphs = env.pipegraphBL.getActivePipegraphs()
  
    //TODO: Maybe we should groupBy another field to avoid duplicates (if exist)...
    val rtComponents = activePipegraphs.flatMap(pg => pg.rtComponents).filter(rt => rt.isActive)
    
    rtComponents
  }

}
