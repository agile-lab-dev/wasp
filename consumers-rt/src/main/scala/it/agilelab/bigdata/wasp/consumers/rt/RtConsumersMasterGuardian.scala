package it.agilelab.bigdata.wasp.consumers.rt

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.core.models.RTModel
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


class RtConsumersMasterGuardian(env: {
                                       val pipegraphBL: PipegraphBL
                                       val topicBL: TopicBL
                                       val indexBL: IndexBL
                                       val websocketBL: WebsocketBL
                                     })
    extends Stash
    with Logging
    with WaspConfiguration {
  // counters for components
  private var rtTotal = 0
  
  // counter for ready components
  private var numberOfReadyComponents = 0
  
  // tracking map for rt components ( componentName -> RTActor )
  private val rtComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
  
  // ActorRef to MasterGuardian returned by the last ask - cannot be replaced with a simple ActorRef or singleton proxy!
  private var masterGuardian: ActorRef = _
  
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
  def uninitialized: Actor.Receive = {
    case RestartConsumers =>
      // update MasterGuardian ActorRef
      masterGuardian = sender()
      
      beginStartup()
  }
  
  // behaviour while starting
  def starting: Actor.Receive = {
    case OutputStreamInitialized =>
      // register component actor
      registerComponentActor(sender())
      
      if (numberOfReadyComponents == rtTotal) {
        // all component actors registered; finish startup
        logger.info(s"All consumer $numberOfReadyComponents child actors have registered! Continuing startup sequence...")
        finishStartup()
      } else {
        logger.info(s"Not all component actors have registered to the cluster (right now only $numberOfReadyComponents " +
                      s"out of $rtTotal), waiting for more...")
      }
    case RestartConsumers =>
      logger.info(s"Stashing RestartConsumers from ${sender()}")
      stash()
  }
  
  // behavior once initialized
  def initialized: Actor.Receive = {
    case RestartConsumers =>
      // update MasterGuardian ActorRef
      masterGuardian = sender()
      
      // attempt stopping
      val stopSuccessful = stop()
  
      // only proceed with restart if we actually stopped
      if (stopSuccessful) {
        beginStartup()
      }
  }
  
  // methods implementing start/stop ===================================================================================
  
  private def beginStartup(): Unit = {
    logger.info(s"RtConsumersMasterGuardian $self beginning startup sequence...")
    
    // find all rt components marked as active, save how many they are in state
    val activeRtComponents = findActiveRtComponents
    rtTotal = activeRtComponents.size
    
    if (activeRtComponents.isEmpty) {
      // no rts to start
      logger.info(s"Found $rtTotal RT components marked as active; entering uninitialized state")
      context become uninitialized
      logger.info("Sending success to master guardian")
      masterGuardian ! true
      logger.info("Startup sequence completed, no RT components started")
    } else {
      // start rts
      logger.info(s"Found $rtTotal RT components marked as active; entering starting state")
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
      // all component actors started; now we wait for them to send us back all the OutputStreamInitialized messages
      logger.info(s"RtConsumersMasterGuardian $self pausing startup sequence, waiting for all component actors to register...")
    }
  }
  
  private def registerComponentActor(componentActor: ActorRef): Unit = {
    logger.info(s"Component actor $componentActor registered")
    numberOfReadyComponents += 1
  }
  
  private def finishStartup(): Unit = {
    logger.info(s"RtConsumersMasterGuardian $self continuing startup sequence...")
    
    // confirm startup success to MasterGuardian
    masterGuardian ! true
  
    // enter intialized state
    context become initialized
    logger.info(s"RtConsumersMasterGuardian $self is now in initialized state")
  
    // unstash messages stashed while in starting state
    logger.info("Unstashing queued messages...")
    unstashAll()
  }
  
  private def stop(): Boolean = {
    logger.info(s"Stopping...")
    
    // stop all actors bound to this guardian and the guardian itself
    logger.info(s"Stopping child actors bound to this rt consumers master guardian $self")
    val generalTimeoutDuration = generalTimeout.duration
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, generalTimeoutDuration))
    val res = Await.result(globalStatus, generalTimeoutDuration)

    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
      numberOfReadyComponents = 0
      true
    } else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all children")
      numberOfReadyComponents = context.children.size
      logger.error(s"Found $numberOfReadyComponents children still running")
      masterGuardian ! false
      false
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
