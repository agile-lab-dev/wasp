package it.agilelab.bigdata.wasp.consumers.rt

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.core.models.RTModel
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


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
      stopGuardian()
      startGuardian()
  }
  
  // overriden ClusterAwareNodeGuardian methods ========================================================================
  
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

  private def stopGuardian() {
    logger.info(s"Stopping...")
    
    // stop all actors bound to this guardian and the guardian itself
    logger.info(s"Stopping child actors bound to this rt consumers master guardian $self")
    val timeout = waspConfig.generalTimeoutMillis milliseconds
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, timeout))
    val res = Await.result(globalStatus, timeout)

    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
      numActiveRtComponents = 0
    }
    else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all children")
    }
  }

  private def startGuardian(): Unit = {
    logger.info(s"Starting up...")
    
    // find all rt components marked as active, save how many they are in state
    val activeRtComponents = loadActiveRtComponents
    targetNumActiveRtComponents = activeRtComponents.size
    
    if (activeRtComponents.isEmpty) {
      logger.info(s"Found $targetNumActiveRtComponents RT components marked as active; entering uninitialized state")
      context become uninitialized
      logger.info("Sending success to master guardian")
      lastRestartMasterRef ! true
      logger.info("Startup sequence completed, no RT components started")
    } else {
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

  private def loadActiveRtComponents: Seq[RTModel] = {
    logger.info(s"Loading all active RT components...")
    
    val activePipegraphs = env.pipegraphBL.getActivePipegraphs()
  
    //TODO: Maybe we should groupBy another field to avoid duplicates (if exist)...
    val rtComponents = activePipegraphs.flatMap(pg => pg.rt).filter(rt => rt.isActive)
    
    rtComponents
  }

}
