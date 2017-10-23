package it.agilelab.bigdata.wasp.consumers.rt

import akka.actor.{ActorRef, Props}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian.generateUniqueComponentName
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
    extends BaseConsumersMasterGuadian(env)
    with WaspConfiguration {
  // counters for components
  private var rtTotal = 0
  
  // getter for total number of components that should be running
  def getTargetNumberOfReadyComponents: Int = rtTotal
  
  // tracking map for rt components ( componentName -> RTActor )
  private val rtComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
  
  // methods implementing start/stop ===================================================================================
  
  
  
  override def beginStartup(): Unit = {
    logger.info(s"RtConsumersMasterGuardian $self beginning startup sequence...")
  
    // gab map containing pipegraph -> components info for active pipegraphs
    logger.info(s"Loading all active pipegraphs...")
    val pipegraphsToComponentsMap = getActivePipegraphsToComponentsMap
    logger.info(s"Found ${pipegraphsToComponentsMap.size} active pipegraphs")
  
    // zero counters for components
    rtTotal = 0
  
    // update counters for components
    pipegraphsToComponentsMap foreach {
      case (pipegraph, (lseComponents, sseComponents, rtComponents)) => {
        // grab sizes
        val rtComponentsListSize = rtComponents.size
      
        // increment size counters for components
        rtTotal += rtComponentsListSize
      
        logger.info(s"Found $rtComponentsListSize total rt components for pipegraph ${pipegraph.name}")
      }
    }
  
    logger.info(s"Found $rtTotal total rt components")
  
    if (rtTotal == 0) { // no active pipegraphs/no components to start
      logger.info("No active pipegraphs with rt components found; aborting startup sequence")
    
      // enter unitizialized state because we don't have anything to do
      context become uninitialized
      logger.info(s"RtConsumersMasterGuardian $self is now in uninitialized state")
    
      // answer ok to MasterGuardian since this is normal if all pipegraphs are unactive
      masterGuardian ! true
    } else { // we have pipegaphs/components to start
      // enter starting state so we stash restarts
      context become starting
      logger.info(s"RtConsumersMasterGuardian $self is now in starting state")
    
      // loop over pipegraph -> components map spawning the appropriate actors for each component
      pipegraphsToComponentsMap foreach {
        case (pipegraph, (lseComponents, sseComponents, rtComponents)) => {
          logger.info(s"Starting component actors for pipegraph ${pipegraph.name}")
        
          // start actors for rt components
          rtComponents.foreach(component => {
            // start component actor
            logger.info(s"Starting RTActor for pipegraph ${pipegraph.name}, component ${component.name}...")
            val actor = context.actorOf(Props(new RTActor(env, component, self)))
            actor ! StartRT
            logger.info(s"Started LegacyStreamingETLActor $actor for pipegraph ${pipegraph.name}, component ${component.name}")
          
            // add to component actor tracking map
            rtComponentActors += (generateUniqueComponentName(pipegraph, component) -> actor)
          })
        
          // do not do anything for legacy streaming/structured streaming components
          logger.info(s"Ignoring ${lseComponents.size} legacy streaming components and ${sseComponents.size} structured " +
                      s"streaming components for pipegraph ${pipegraph.name} as they are handled by " +
                      s"SparkConsumersMasterGuardian")
        }
      }
      
      // all component actors started; now we wait for them to send us back all the OutputStreamInitialized messages
      logger.info(s"RtConsumersMasterGuardian $self pausing startup sequence, waiting for all component actors to register...")
    }
  }
  
  override def finishStartup(): Unit = {
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
  
  override def stop(): Boolean = {
    logger.info(s"Stopping...")
  
    // stop all component actors bound to this guardian and the guardian itself
    logger.info(s"Stopping component actors bound to RtConsumersMasterGuardian $self...")
  
    // find and stop all RTActors belonging to pipegraphs that are no longer active
    // get the component names for all components of all active pipegraphs
    val activeRTComponentNames = getActivePipegraphsToComponentsMap flatMap {
      case (pipegraph, (_, _, rtComponents)) => {
        rtComponents map { component => generateUniqueComponentName(pipegraph, component) }
      }
    } toSet
    // diff with the set of component names for component actors to find the ones we have to stop
    val inactiveRTComponentNames = rtComponentActors.keys.toSet.diff(activeRTComponentNames)
    // grab corresponding actor refs
    val inactiveRTComponentActors = inactiveRTComponentNames.map(rtComponentActors).toSeq
    // gracefully stop all component actors corresponding to now-inactive pipegraphs
    logger.info(s"Gracefully stopping ${inactiveRTComponentActors.size} rt component actors managing now-inactive components...")
    val generalTimeoutDuration = generalTimeout.duration
    val rtStatuses = inactiveRTComponentActors.map(gracefulStop(_, generalTimeoutDuration))
  
    // await all component actors' stopping
    val res = Await.result(Future.sequence(rtStatuses), generalTimeoutDuration)
  
    // check whether all components actors that had to stop actually stopped
    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
    
      // cleanup references to now stopped component actors
      inactiveRTComponentNames.map(rtComponentActors.remove) // remove rt components actors that we stopped
    
      // update counter for ready components because some rt components actors might have been left running
      numberOfReadyComponents = rtComponentActors.size
    
      // no message sent to the MasterGuardian because we still need to startup again after this
    
      true
    } else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all components actors")
    
      // find out which children are still running
      val childrenSet = context.children.toSet
      numberOfReadyComponents = childrenSet.size
      logger.error(s"Found $numberOfReadyComponents children still running")
    
      // filter out component actor tracking maps
      val filteredRTCA = rtComponentActors filter { case (name, actor) => childrenSet(actor) }
      rtComponentActors.clear()
      rtComponentActors ++= filteredRTCA
    
      // output info about component actors still running
      rtComponentActors foreach {
        case (name, actor) => logger.error(s"RT component actor $actor for component $name is still running")
      }
    
      // tell the MasterGuardian we failed
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
