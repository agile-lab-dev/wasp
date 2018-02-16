package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.{ActorRef, Props}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{StreamingReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian.generateUniqueComponentName
import it.agilelab.bigdata.wasp.core.messages.StopProcessingComponent
import it.agilelab.bigdata.wasp.core.utils.{SparkStreamingConfiguration, WaspConfiguration}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}


// TODO: uninitialized/initialized/starting handle different messages; are we sure can we just let everything else go into the dead letters *safely*?
class SparkConsumersMasterGuardian(env: {
                                      val producerBL: ProducerBL
                                      val pipegraphBL: PipegraphBL
                                      val topicBL: TopicBL
                                      val indexBL: IndexBL
                                      val rawBL: RawBL
                                      val keyValueBL: KeyValueBL
                                      val websocketBL: WebsocketBL
                                      val mlModelBL: MlModelBL
                                    },
                                    sparkWriterFactory: SparkWriterFactory,
                                    streamingReader: StreamingReader,
                                    structuredStreamingReader: StructuredStreamingReader,
                                    plugins: Map[String, WaspConsumersSparkPlugin])
  extends BaseConsumersMasterGuadian(env)
    with SparkStreamingConfiguration
    with WaspConfiguration {

  // counters for components
  private var legacyStreamingETLTotal = 0
  private var structuredStreamingETLTotal = 0
  
  // getter for total number of components that should be running
  def getTargetNumberOfReadyComponents: Int = legacyStreamingETLTotal + structuredStreamingETLTotal
  
  // tracking map for structured streaming components ( componentName -> StructuredStreamingETLActor )
  private val ssComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
  
  // tracking map for legacy streaming components ( componentName -> LegacyStreamingETLActor )
  private val lsComponentActors: mutable.Map[String, ActorRef] = mutable.Map.empty[String, ActorRef]
  
  // actor lifecycle callbacks =========================================================================================
  
  override def preStart(): Unit = {
    // initialize Spark
    val scCreated = SparkSingletons.initializeSpark(sparkStreamingConfig)
    if (!scCreated) logger.warn("Spark was already initialized: it might not be using the spark streaming configuration!")
  }
  
  override def postStop(): Unit = {
    // TODO: the last 2 lines use blocking calls without timeouts... maybe use the ones with a timeout?
    // stop all streaming
    SparkSingletons.getStreamingContext.stop(stopSparkContext = false, stopGracefully = false)
    SparkSingletons.getStreamingContext.awaitTermination()
    SparkSingletons.getSparkSession.streams.active.foreach(_.stop())
  }
  
  // methods implementing start/stop ===================================================================================

  override def beginStartup(): Unit = {
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
      logger.info("No active pipegraphs with legacy streaming/structured streaming components found; aborting startup sequence")
      
      // enter unitizialized state because we don't have anything to do
      context become uninitialized
      logger.info(s"SparkConsumersMasterGuardian $self is now in uninitialized state")
      
      // answer ok to MasterGuardian since this is normal if all pipegraphs are unactive
      masterGuardian ! Right()

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
            val actor = context.actorOf(Props(new LegacyStreamingETLActor(env, sparkWriterFactory, streamingReader, ssc, pipegraph, component, self, plugins)))
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
          logger.info(s"Ignoring ${rtComponents.size} rt components " +
                      s"for pipegraph ${pipegraph.name} as they are handled by RtConsumersMasterGuardian")
        }
      }
  
      if (numberOfReadyComponents == getTargetNumberOfReadyComponents) {
        // all component actors registered; finish startup
        logger.info(s"All $numberOfReadyComponents consumer child actors are already running! Continuing startup sequence...")
        finishStartup()
      } else {
        // all component actors started; now we wait for them to send us back all the Right messages
        logger.info(s"SparkConsumersMasterGuardian $self pausing startup sequence, waiting for all component actors to register...")
      }
    }
  }
  
  override def finishStartup(success: Boolean = true, errorMsg: String = ""): Unit = {
    if (success) {
      logger.info(s"SparkConsumersMasterGuardian $self continuing startup sequence...")

      if (legacyStreamingETLTotal > 0) {
        logger.info("Starting StreamingContext...")
        SparkSingletons.getStreamingContext.start()
        logger.info("Started StreamingContext")
      } else {
        logger.info("Not starting StreamingContext because no legacy streaming components are present")
      }

      // confirm startup success to MasterGuardian
      masterGuardian ! Right()

      // enter initialized state
      context become initialized
      logger.info(s"SparkConsumersMasterGuardian $self is now in initialized state")
    } else {
      logger.info(s"SparkConsumersMasterGuardian $self stopping startup sequence...")

      // startup error to MasterGuardian
      masterGuardian ! Left(errorMsg)

      // enter uninitialized state
      context become uninitialized
      logger.info(s"SparkConsumersMasterGuardian $self is now in uninitialized state")
    }

    // unstash messages stashed while in starting state
    logger.info(s"SparkConsumersMasterGuardian $self unstashing queued messages...")
    unstashAll()

    // TODO check if this is still needed in Spark 2.x
    // sleep to avoid quick start/stop/start of StreamingContext which breaks with timeout errors
    Thread.sleep(5 * 1000)
  }
  
  override def stop(): Boolean = {
    logger.info(s"SparkConsumersMasterGuardian $self stopping...")
  
    // stop all component actors bound to this guardian and the guardian itself
    logger.info(s"Stopping component actors bound to SparkConsumersMasterGuardian $self...")
  
    // stop StreamingContext (if needed) and all LegacyStreamingETLActor
    if (legacyStreamingETLTotal > 0) {
      logger.info("Stopping StreamingContext...")
      SparkSingletons.getStreamingContext.stop(stopSparkContext = false, stopGracefully = true)
      SparkSingletons.getStreamingContext.awaitTermination()
      SparkSingletons.deinitializeSparkStreaming()
      logger.info("Stopped StreamingContext")
    } else {
      logger.info("Not stopping StreamingContext because no legacy streaming components are present")
    }
  
    // gracefully stop all component actors corresponding to legacy components
    logger.info(s"Gracefully stopping ${lsComponentActors.size} legacy streaming component actors...")
    import scala.concurrent.duration._
    val timeoutDuration = generalTimeout.duration - 15.seconds
    val legacyStreamingStatuses = lsComponentActors.values.map(gracefulStop(_, timeoutDuration - 5.seconds))
  
    // gracefully stop all StructuredStreamingETLActors belonging to pipegraphs that are no longer active
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
    val structuredStreamingStatuses = inactiveStructuredStreamingComponentActors.map(gracefulStop(_, timeoutDuration - 5.seconds, StopProcessingComponent))
    
    val globalStatuses = legacyStreamingStatuses ++ structuredStreamingStatuses
  
    if (globalStatuses.nonEmpty) {
      logger.info(s"Waiting for ${globalStatuses.size} component actors to stop...")

      try {
        // await all component actors' stopping
        val res = Await.result(Future.sequence(globalStatuses), timeoutDuration)

        // check whether all components actors that had to stop actually stopped
        if (res reduceLeft (_ && _)) {
          logger.info(s"Stopping sequence completed, ${globalStatuses.size} component actors stopped")

          // cleanup references to now stopped component actors
          lsComponentActors.clear() // remove all legacy streaming components actors
          inactiveStructuredStreamingComponentNames.map(ssComponentActors.remove) // remove structured streaming components actors that we stopped

          // update counter for ready components because some structured streaming components actors might have been left running
          numberOfReadyComponents = ssComponentActors.size

          // no message sent to the MasterGuardian because we still need to startup again after this

          true
        } else {
          val msg = "Stopping sequence failed! Unable to shutdown all components actors"
          logger.error(msg)

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
          masterGuardian ! Left(msg)

          false
        }
      } catch {
        case e: Exception =>
          val msg = s"Streaming component actors not all stopped - Exception: ${e.getMessage}"
          logger.error(msg, e)
          masterGuardian ! Left(msg)

          false
      }
    } else {
      // no component actors to stop
      logger.info(s"Stopping sequence completed, no component actors to stop")
  
      // no message sent to the MasterGuardian because we still need to startup again after this
      
      true
    }
  }
}
