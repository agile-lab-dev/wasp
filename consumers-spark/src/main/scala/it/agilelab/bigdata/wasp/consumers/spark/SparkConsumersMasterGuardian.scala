package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{StreamingReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils._
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.core.models.{LegacyStreamingETLModel, PipegraphModel, RTModel, StructuredStreamingETLModel}
import it.agilelab.bigdata.wasp.core.utils.{SparkStreamingConfiguration, WaspConfiguration}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}


// TODO: uninitialized/initialized/starting handle different messages; are we sure can we just let everything else go into the dead letters *safely*?
class SparkConsumersMasterGuardian(env: {val producerBL: ProducerBL
                                         val pipegraphBL: PipegraphBL
                                         val topicBL: TopicBL
                                         val indexBL: IndexBL
                                         val rawBL: RawBL
                                         val keyValueBL: KeyValueBL
                                         val websocketBL: WebsocketBL
                                         val mlModelBL: MlModelBL},
                                   sparkWriterFactory: SparkWriterFactory,
                                   streamingReader: StreamingReader,
                                   structuredStreamingReader: StructuredStreamingReader,
                                   plugins: Map[String, WaspConsumerSparkPlugin])
    extends SparkStreamingConfiguration
    with Stash
    with Logging
    with WaspConfiguration {
  // type alias for pipegraph -> components map
  type PipegraphsToComponentsMap = Map[PipegraphModel, (Seq[LegacyStreamingETLModel], Seq[StructuredStreamingETLModel], Seq[RTModel])]
  
  // counters for components
  private var legacyStreamingETLTotal = 0
  private var structuredStreamingETLTotal = 0
  
  // counter for ready components
  private var numberOfReadyComponents = 0
  
  // tracking map for structured streaming components ( componentName -> StructuredStreamingETLActor )
  private val ssComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
  
  // tracking map for legacy streaming components ( componentName -> LegacyStreamingETLActor )
  private val lsComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]

  // ActorRef to MasterGuardian returned by the last ask - cannot be replaced with a simple ActorRef or singleton proxy!
  private var masterGuardian: ActorRef = _
  
  // actor lifecycle callbacks =========================================================================================
  
  override def preStart(): Unit = {
    // initialize Spark
    val scCreated = SparkSingletons.initializeSpark(sparkStreamingConfig)
    if (!scCreated) logger.warn("Spark was already initialized: it might not be using the spark streaming configuration!")
  
    // we start in uninitialized state
    context become uninitialized
  }
  
  override def postStop(): Unit = {
    // TODO: the last 2 lines use blocking calls without timeouts... maybe use the ones with a timeout?
    // stop all streaming
    SparkSingletons.getStreamingContext.stop(stopSparkContext = false, stopGracefully = false)
    SparkSingletons.getStreamingContext.awaitTermination()
    SparkSingletons.getSparkSession.streams.active.foreach(_.stop())
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
      
      if (numberOfReadyComponents == (legacyStreamingETLTotal + structuredStreamingETLTotal)) {
        // all component actors registered; finish startup
        logger.info(s"All $numberOfReadyComponents consumer child actors have registered! Continuing startup sequence...")
        finishStartup()
      } else {
        logger.info(s"Not all component actors have registered to the cluster (right now only $numberOfReadyComponents " +
                    s"out of ${legacyStreamingETLTotal + structuredStreamingETLTotal}), waiting for more...")
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
      val stoppingSuccessful = stop()
      
      // only proceed with restart if we actually stopped
      if (stoppingSuccessful) {
        beginStartup()
      }
  }
  
  // methods implementing start/stop ===================================================================================

  private def beginStartup() {
    logger.info(s"SparkConsumersMasterGuardian $self beginning startup sequence...")

    SparkSingletons.initializeSparkStreaming(sparkStreamingConfig)
    val ssc = SparkSingletons.getStreamingContext
    val ss = SparkSingletons.getSparkSession

    // gab map containing pipegraph -> components info for active pipegraphs
    logger.info(s"Loading all active pipegraphs...")
    val pipegraphsToComponentsMap = getActivePipegraphsToComponentsMap
    logger.info(s"Found ${pipegraphsToComponentsMap.size} active pipegraphs")
  
    // zero counters for components
    legacyStreamingETLTotal = 0
    structuredStreamingETLTotal = 0
    
    // update counters for components
    pipegraphsToComponentsMap foreach {
      case (pipegraph, (lseComponents, sseComponents, rtComponents)) => {
        // grab sizes
        val lseListSize = lseComponents.size
        val sseListSize = sseComponents.size
    
        // increment size counters for components
        legacyStreamingETLTotal += lseListSize
        structuredStreamingETLTotal += sseListSize
    
        logger.info(s"Found ${lseListSize + sseListSize} total components for pipegraph ${pipegraph.name} " +
                      s"($lseListSize legacy streaming, $sseListSize structured streaming)")
      }
    }
  
    logger.info(s"Found ${legacyStreamingETLTotal + structuredStreamingETLTotal} total components " +
                  s"($legacyStreamingETLTotal legacy streaming, $structuredStreamingETLTotal structured streaming)")
  
    if (legacyStreamingETLTotal + structuredStreamingETLTotal == 0) { // no active pipegraphs/no components to start
      logger.info("No active pipegraphs/components found; aborting startup sequence")
      
      // enter unitizialized state because we don't have anything to do
      context become uninitialized
      logger.info(s"SparkConsumersMasterGuardian $self is now in uninitialized state")
      
      // answer ok to MasterGuardian since this is normal if all pipegraphs are unactive
      masterGuardian ! true
    } else { // we have pipegaphs/components to start
      // enter starting state so we stash restarts
      context become starting
      logger.info(s"SparkConsumersMasterGuardian $self is now in starting state")
      
      // loop over pipegraph -> components map spawning the appropriate actors for each component
      pipegraphsToComponentsMap foreach {
        case (pipegraph, (lseComponents, sseComponents, rtComponents)) => {
          logger.info(s"Starting component actors for pipegraph ${pipegraph.name}")
          
          // start actors for legacy streaming components
          lseComponents.foreach(component => {
            // start component actor
            logger.info(s"Starting LegacyStreamingETLActor for pipegraph ${pipegraph.name}, component ${component.name}...")
            val actor = context.actorOf(Props(new LegacyStreamingETLActor(env, sparkWriterFactory, streamingReader, ssc, component, self, plugins)))
            logger.info(s"Started LegacyStreamingETLActor $actor for pipegraph ${pipegraph.name}, component ${component.name}")
  
            // add to component actor tracking map
            lsComponentActors += (generateUniqueComponentName(pipegraph, component) -> actor)
          })
  
          // start actors for structured streaming components if they are not already started
          sseComponents.foreach(component => {
            val structuredQueryName = generateUniqueComponentName(pipegraph, component)
            if (ssComponentActors.contains(structuredQueryName)) {
              // component actor already running, skip component
              logger.info(s"Component actor for pipegraph ${pipegraph.name}, component ${component.name} already exists " +
                          s"as StructuredStreamingETLActor ${ssComponentActors(structuredQueryName)}, " +
                          s"skipping creation")
            } else {
              // start component actor
              logger.info(s"Starting StructuredStreamingETLActor for pipegraph ${pipegraph.name}, component ${component.name}...")
              val actor = context.actorOf(Props(new StructuredStreamingETLActor(env, sparkWriterFactory, structuredStreamingReader, ss, pipegraph, component, self, plugins)))
              logger.info(s"Started StructuredStreamingETLActor $actor for pipegraph ${pipegraph.name}, component ${component.name}")
              
              // add to component actor tracking map
              ssComponentActors += (structuredQueryName -> actor)
            }
          })
          
          // do not do anything for rt components
          logger.info(s"Ignoring ${rtComponents.size} rt components for pipegraph ${pipegraph.name} as they are handled by RtConsumersMasterGuardian")
        }
      }
  
      // all component actors started; now we wait for them to send us back all the OutputStreamInitialized messages
      logger.info(s"SparkConsumersMasterGuardian $self pausing startup sequence, waiting for all component actors to register...")
    }
  }
  
  private def registerComponentActor(componentActor: ActorRef): Unit = {
    logger.info(s"Component actor $componentActor registered")
    numberOfReadyComponents += 1
  }
  
  private def finishStartup(): Unit = {
    logger.info(s"SparkConsumersMasterGuardian $self continuing startup sequence...")

    if (legacyStreamingETLTotal > 0) {
      logger.info("Starting StreamingContext...")
      SparkSingletons.getStreamingContext.start()
      logger.info("Started StreamingContext")
    } else {
      logger.info("Not starting StreamingContext because no legacy streaming components are present")
    }
    
    // confirm startup success to MasterGuardian
    masterGuardian ! true
    
    // enter intialized state
    context become initialized
    logger.info(s"SparkConsumersMasterGuardian $self is now in initialized state")
    
    // unstash messages stashed while in starting state
    logger.info("Unstashing queued messages...")
    unstashAll()
    
    // TODO check if this is still needed in Spark 2.x
    // sleep to avoid quick star/stop/start of StreamingContext which breaks with timeout errors
    Thread.sleep(5 * 1000)
  }
  
  private def stop(): Boolean = {
    logger.info(s"SparkConsumersMasterGuardian $self stopping...")
    
    // stop all component actors bound to this guardian and the guardian itself
    logger.info(s"Stopping component actors bound to SparkConsumersMasterGuardian $self...")
    
    // stop StreamingContext and all LegacyStreamingETLActor
    // stop streaming context
    SparkSingletons.getStreamingContext.stop(stopSparkContext = false, stopGracefully = true)
    SparkSingletons.getStreamingContext.awaitTermination()
    SparkSingletons.deinitializeSparkStreaming()
    // gracefully stop all component actors corresponding to legacy components
    logger.info(s"Gracefully stopping all ${lsComponentActors.size} legacy streaming component actors...")
    val generalTimeoutDuration = generalTimeout.duration
    val legacyStreamingStatuses = lsComponentActors.values.map(gracefulStop(_, generalTimeoutDuration))
    
    // find and stop StructuredStreamingETLActor belonging to pipegraphs that are no longer active
    // get the component names for all components of all active pipegraphs
    val activeStructuredStreamingComponentNames = getActivePipegraphsToComponentsMap flatMap {
      case (pipegraph, (_, sseComponents, _)) => {
        sseComponents map { component => generateUniqueComponentName(pipegraph, component) }
      }
    } toSet
    // diff with the set of component names for component actors to find the ones we have to stop
    val inactiveStructuredStreamingComponentNames = ssComponentActors.keys.toSet.diff(activeStructuredStreamingComponentNames)
    // grab corresponding actor refs
    val inactiveStructuredStreamingComponentActors = inactiveStructuredStreamingComponentNames.map(ssComponentActors).toSeq
    // gracefully stop all component actors corresponding to now-inactive pipegraphs
    logger.info(s"Gracefully stopping ${inactiveStructuredStreamingComponentActors.size} structured streaming component actors managing now-inactive components...")
    val structuredStreamingStatuses = inactiveStructuredStreamingComponentActors.map(gracefulStop(_, generalTimeoutDuration))
    
    // await all component actors' stopping
    val globalStatuses = legacyStreamingStatuses ++ structuredStreamingStatuses
    val res = Await.result(Future.sequence(globalStatuses), generalTimeoutDuration)
    
    // check whether all components actors that had to stop actually stopped
    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
      
      // cleanup references to now stopped component actors
      lsComponentActors.clear() // remove all legacy streaming components actors
      inactiveStructuredStreamingComponentNames.map(ssComponentActors.remove) // remove structured streaming components actors that we stopped
      
      // update counter for ready components because some structured streaming components actors might have been left running
      numberOfReadyComponents = ssComponentActors.size
      
      // no message sent to the MasterGuardian because we still need to startup again after this
      
      true
    } else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all components actors")
      
      // find out which children are still running
      val childrenSet = context.children.toSet
      numberOfReadyComponents = childrenSet.size
      logger.error(s"Found $numberOfReadyComponents children still running")
  
      // filter out component actor tracking maps
      val filteredLSCA = lsComponentActors filter { case (name, actor) => childrenSet(actor) }
      lsComponentActors.clear()
      lsComponentActors ++= filteredLSCA
      val filteredSSCA = ssComponentActors filter { case (name, actor) => childrenSet(actor) }
      ssComponentActors.clear()
      ssComponentActors ++= filteredSSCA
      
      // output info about component actors still running
      lsComponentActors foreach {
        case (name, actor) => logger.error(s"Legacy streaming component actor $actor for component $name is still running")
      }
      ssComponentActors foreach {
        case (name, actor) => logger.error(s"Structured streaming component actor $actor for component $name is still running")
      }
  
      // tell the MasterGuardian we failed
      masterGuardian ! false
      
      false
    }
  }
  
  // helper methods ====================================================================================================

  private def getActivePipegraphsToComponentsMap: PipegraphsToComponentsMap = {
    val pipegraphs: Seq[PipegraphModel] = env.pipegraphBL.getActivePipegraphs()
    
    // extract components from active pipegraphs as a Map pipegraph -> components
    val pipegraphsToComponentsMap: PipegraphsToComponentsMap = pipegraphs map {
      pipegraph => {
        // grab components
        val lseComponents = pipegraph.legacyStreamingComponents
        val sseComponents = pipegraph.structuredStreamingComponents
        val rtComponents = pipegraph.rtComponents
        
        pipegraph -> (lseComponents, sseComponents, rtComponents)
      }
    } toMap
    
    pipegraphsToComponentsMap
  }

}
