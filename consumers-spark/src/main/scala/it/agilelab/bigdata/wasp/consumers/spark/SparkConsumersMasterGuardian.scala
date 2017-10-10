package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{StreamingReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.core.models.{PipegraphModel, RTModel, LegacyStreamingETLModel, StructuredStreamingETLModel}
import it.agilelab.bigdata.wasp.core.utils.{SparkStreamingConfiguration, WaspConfiguration}

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
    extends ClusterAwareNodeGuardian
    with SparkStreamingConfiguration
    with Stash
    with Logging
    with WaspConfiguration {
  // type alias for pipegraph -> components map
  type PipegraphsToComponentsMap = Map[PipegraphModel, (Seq[LegacyStreamingETLModel], Seq[StructuredStreamingETLModel], Seq[RTModel])]
  
  // counters for components
  var legacyStreamingETLTotal = 0
  var structuredStreamingETLTotal = 0
  var rtTotal = 0
  
  // counter for ready components
  var numberOfReadyComponents = 0

  /** STARTUP PHASE **/
  /** *****************/

    // TODO move this stuff in preStart()
  // initialize Spark
  val scCreated = SparkSingletons.initializeSpark(sparkStreamingConfig)
  if (!scCreated) logger.warn("Spark was already initialized: it might not be using the spark streaming configuration!")

  // we start in uninitialized state
  context become uninitialized

  // TODO: use proxy from waspsystem
  private var lastRestartMasterRef: ActorRef = _
  
  // TODO: wipe this
  override def preStart(): Unit = {
    super.preStart()
    //TODO:capire joinseednodes
    cluster.joinSeedNodes(Vector(cluster.selfAddress))
  }
  
  // behaviours ========================================================================================================

  // behaviour when uninitialized
  override def uninitialized: Actor.Receive = {
    case RestartConsumers =>
      lastRestartMasterRef = sender()
      beginStartup()
  }

  // behaviour while starting
  def starting: Actor.Receive = {
    case OutputStreamInitialized =>
      // register component actor
      registerComponentActor(sender())
      
      if (numberOfReadyComponents == (legacyStreamingETLTotal + structuredStreamingETLTotal)) {
        // all component actors registered; finish startup
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
  override def initialized: Actor.Receive = {
    case RestartConsumers =>
      lastRestartMasterRef = sender()
      // attempt stopping
      val stoppingSuccessful = stopGuardian()
      
      // only proceed with restart if we actually stopped
      if (stoppingSuccessful) {
        beginStartup()
      }
  }
  
  // methods implementing start/stop ===================================================================================

  private def beginStartup() {
    logger.info(s"SparkConsumersMasterGuardian $self starting...")

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
    rtTotal = 0
    
    // update counters for components
    pipegraphsToComponentsMap foreach {
      case (pipegraph, (lseComponents, sseComponents, rtComponents)) => {
        // grab sizes
        val lseListSize = lseComponents.size
        val sseListSize = sseComponents.size
        val rtListSize = rtComponents.size
    
        // increment size counters for components
        legacyStreamingETLTotal += lseListSize
        structuredStreamingETLTotal += sseListSize
        rtTotal += rtListSize
    
        logger.info(s"Found ${lseListSize + sseListSize + rtListSize} total components for pipegraph ${pipegraph.name} " +
                      s"($lseListSize legacy streaming, $sseListSize structured streaming, $rtListSize rt)")
      }
    }
  
    logger.info(s"Found ${legacyStreamingETLTotal + structuredStreamingETLTotal + rtTotal} total components " +
                  s"($legacyStreamingETLTotal legacy streaming, $structuredStreamingETLTotal structured streaming, " +
                  s"$rtTotal rt)")
  
    if (legacyStreamingETLTotal + structuredStreamingETLTotal == 0) { // no active pipegraphs/no components to start
      logger.info("No active pipegraphs/components found; aborting startup sequence")
      
      // enter unitizialized state because we don't have anything to do
      context become uninitialized
      logger.info(s"SparkConsumersMasterGuardian $self is now in uninitialized state")
      
      // answer ok to MasterGuardian since this is normal if all pipegraphs are unactive
      lastRestartMasterRef ! true
    } else { // we have pipegaphs/components to start
      // enter starting state so we stash restarts
      context become starting
      logger.info(s"SparkConsumersMasterGuardian $self is now in starting state")
      
      // loop over pipegraph -> components map spawning the appropriate actors for each component
      pipegraphsToComponentsMap foreach {
        case (pipegraph, (lseComponents, sseComponents, rtComponents)) => {
          logger.info(s"Starting *StreaminETLActors for pipegraph ${pipegraph.name}")
          
          // start actors for legacy streaming components
          lseComponents.foreach(component => {
            logger.info(s"Starting LegacyStreamingETLActor for pipegraph ${pipegraph.name}, component ${component.name}...")
            val actor = context.actorOf(Props(new LegacyStreamingETLActor(env, sparkWriterFactory, streamingReader, ssc, component, self, plugins)))
            logger.info(s"Started LegacyStreamingETLActor $actor for pipegraph ${pipegraph.name}, component ${component.name}")
          })
  
          // start actors for structured streaming components
          sseComponents.foreach(component => {
            logger.info(s"Starting StructuredStreamingETLActor for pipegraph ${pipegraph.name}, component ${component.name}...")
            val actor = context.actorOf(Props(new StructuredStreamingETLActor(env, sparkWriterFactory, structuredStreamingReader, ss, null, component, self, plugins)))
            logger.info(s"Started StructuredStreamingETLActor $actor for pipegraph ${pipegraph.name}, component ${component.name}")
          })
          
          // do not do anything for rt components
          logger.info(s"Ignoring ${rtComponents.size} rt components for pipegraph ${pipegraph.name} as they are handled by RtConsumersMasterGuardian")
        }
      }
  
      // all component actors started; now we wait for them to send us back all the OutputStreamInitialized messages
      logger.info("Waiting for all component actors to register...")
    }
  }
  
  private def registerComponentActor(componentActor: ActorRef): Unit = {
    logger.info(s"Component actor $componentActor registered")
    numberOfReadyComponents += 1
  }
  
  private def finishStartup(): Unit = {
    logger.info("All consumer child actors have registered! Starting StreamingContext")
    SparkSingletons.getStreamingContext.start()
    //questa sleep serve perchè se si fa la stop dello spark streamng context subito dopo che e' stato
    //startato va tutto iin timeout
    // TODO check if this is needed, we lose precious seconds of timeout here
    Thread.sleep(5 * 1000)
    
    // confirm startup success to MasterGuardian
    lastRestartMasterRef ! true
    
    // enter intialized state
    context become initialized
    logger.info(s"SparkConsumersMasterGuardian $self is now in initialized state")
    
    // unstash RestartConsumers stashed while in starting state
    logger.info("Unstashing queued messages...")
    unstashAll()
  }
  
  private def stopGuardian(): Boolean = {
    logger.info(s"SparkConsumersMasterGuardian $self stopping...")
    
    // stop all component actors bound to this guardian and the guardian itself
    logger.info(s"Stopping component actors bound to SparkConsumersMasterGuardian $self...")
    
    // stop StreamingContext
    SparkSingletons.getStreamingContext.stop(stopSparkContext = false, stopGracefully = true)
    SparkSingletons.getStreamingContext.awaitTermination()
    SparkSingletons.deinitializeSparkStreaming()
    
    // TODO stop queries
    ???
    
    val generalTimeoutDuration = generalTimeout.duration
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, generalTimeoutDuration))
    val res = Await.result(globalStatus, generalTimeoutDuration)
    
    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
      numberOfReadyComponents = 0
      true
    }
    else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all components actors")
      numberOfReadyComponents = context.children.size
      logger.error(s"Found $numberOfReadyComponents children still running")
      lastRestartMasterRef ! false
      false
    }
    
  }
  
  // helper methods ====================================================================================================

  //TODO: Maybe we should groupBy another field to avoid duplicates (if exist)...
  private def getActivePipegraphsToComponentsMap: PipegraphsToComponentsMap = {
    val pipegraphs: Seq[PipegraphModel] = env.pipegraphBL.getActivePipegraphs()
    
    // extract components from active pipegraphs as a Map pipegraph -> components
    val pipegraphsToComponentsMap: PipegraphsToComponentsMap = pipegraphs map {
      pipegraph => {
        // grab components
        val lseComponents = pipegraph.legacyStreamingComponents
        val sseComponents = pipegraph.structuredStreamingComponents
        val rtComponents = pipegraph.rtComponents
        
        // grab sizes
        val lseListSize = lseComponents.size
        val sseListSize = sseComponents.size
        val rtListSize = rtComponents.size
        
        pipegraph -> (lseComponents, sseComponents, rtComponents)
      }
    } toMap
    
    pipegraphsToComponentsMap
  }

}
