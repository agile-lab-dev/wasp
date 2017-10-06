package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StreamingReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspSystem.generalTimeout
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.RestartConsumers
import it.agilelab.bigdata.wasp.core.models.{ETLModel, PipegraphModel, RTModel}
import it.agilelab.bigdata.wasp.core.utils.{SparkStreamingConfiguration, WaspConfiguration}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class SparkConsumersMasterGuardian(env: {val producerBL: ProducerBL; val pipegraphBL: PipegraphBL;
  val topicBL: TopicBL; val indexBL: IndexBL
  val rawBL : RawBL; val keyValueBL: KeyValueBL
  val websocketBL: WebsocketBL; val mlModelBL: MlModelBL;},
                                   sparkWriterFactory: SparkWriterFactory,
                                   streamingReader: StreamingReader,
                                   plugins: Map[String, WaspConsumerSparkPlugin])
    extends ClusterAwareNodeGuardian
    with SparkStreamingConfiguration
    with Stash
    with Logging
    with WaspConfiguration {
  
  var etlListSize = 0
  var readyEtls = 0

  /** STARTUP PHASE **/
  /** *****************/

  /** Initialize and retrieve the SparkContext */
  val scCreated = SparkSingletons.initializeSpark(sparkStreamingConfig)
  if (!scCreated) logger.warn("The spark context was already intialized: it might not be using the spark streaming configuration!")
  val sc = SparkSingletons.getSparkContext

  /** Creates the Spark Streaming context. */
  var ssc: StreamingContext = _
  logger.info("Spark streaming context created")

  context become uninitialized

  /** BASIC METHODS **/
  /** *****************/

  private var lastRestartMasterRef: ActorRef = _


  override def preStart(): Unit = {
    super.preStart()
    //TODO:capire joinseednodes
    cluster.joinSeedNodes(Vector(cluster.selfAddress))
  }

  override def initialize(): Unit = {
    logger.info(s"New actor registered with Master!")
    readyEtls = readyEtls + 1

    // Until all children aren't ready to stream we don't start the SSC
    if (readyEtls == etlListSize) {
      super.initialize()
      logger.info("All consumer child actors have sucessfully connected to the master guardian! Starting SSC")
      ssc.checkpoint(sparkStreamingConfig.checkpointDir)
      ssc.start()
      Thread.sleep(5 * 1000)
      lastRestartMasterRef ! true
      context become initialized
      logger.info("SparkConsumerMasterGuardian Initialized")
      logger.info("Unstashing queued messages...")
      unstashAll()
    }
    else {
      logger.info(s"Not all actors have registered to the cluster (right now [$readyEtls]), waiting for more ...")
    }
  }

  override def uninitialized: Actor.Receive = {

    case RestartConsumers =>
      lastRestartMasterRef = sender()
      startGuardian()
  }

  def starting: Actor.Receive = {
    case OutputStreamInitialized => initialize()
    case RestartConsumers =>
      logger.info(s"Stashing restart ...")
      stash()
  }

  /** This node guardian's customer behavior once initialized. */
  override def initialized: Actor.Receive = {
    case RestartConsumers =>
      lastRestartMasterRef = sender()
      val stoppingSuccessful = stopGuardian()
      if (stoppingSuccessful) {
        startGuardian()
      }
  }

  /** PRIVATE METHODS **/
  /** ******************/

  private def stopGuardian(): Boolean = {
    logger.info(s"Stopping...")
  
    // stop all actors bound to this guardian and the guardian itself
    logger.info(s"Stopping child actors bound to this spark consumers master guardian $self")

    //questa sleep serve perchè se si fa la stop dello spark streamng context subito dopo che e' stato
    //startato va tutto iin timeout
    //TODO capire come funziona
    //Thread.sleep(1500)
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc.awaitTermination()
  
    val generalTimeoutDuration = generalTimeout.duration
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, generalTimeoutDuration))
    val res = Await.result(globalStatus, generalTimeoutDuration)

    if (res reduceLeft (_ && _)) {
      logger.info(s"Stopping sequence completed")
      readyEtls = 0
      true
    }
    else {
      logger.error(s"Stopping sequence failed! Unable to shutdown all nodes")
      readyEtls = context.children.size
      logger.error(s"Found $readyEtls children still running")
      lastRestartMasterRef ! false
      false
    }

  }

  private def startGuardian() {

    logger.info(s"Starting ConsumersMasterGuardian actors...")

    ssc = new StreamingContext(sc, Milliseconds(sparkStreamingConfig.streamingBatchIntervalMs))

    logger.info(s"Streaming context created...")

    val (activeETL, activeRT) = loadActivePipegraphs
    //TODO Why no pipegraphs with only RT modules?
    //if (activeETL.isEmpty) {
    if (activeETL.isEmpty && activeRT.isEmpty) {
      context become uninitialized
      logger.info("ConsumerMasterGuardian Uninitialized")
      lastRestartMasterRef ! true
    } else {
      context become starting
      logger.info("ConsumerMasterGuardian Starting")
      activeETL.map(element => {
        logger.info(s"***Starting Streaming Etl actor [${element.name}]")
        context.actorOf(Props(new ConsumerEtlActor(env, sparkWriterFactory, streamingReader, ssc, element, self, plugins)))
      })
      //TODO If statement added to handle pipegraphs with only RT components, cleaner way to do this to be found
      if(activeETL.isEmpty)
        {
          lastRestartMasterRef ! true
          context become initialized
        }
    }
  }

  //TODO: Maybe we should groupBy another field to avoid duplicates (if exist)...
  private def loadActivePipegraphs: (Seq[ETLModel], Seq[RTModel]) = {
    logger.info(s"Loading all active Pipegraphs ...")
    val pipegraphs: Seq[PipegraphModel] = env.pipegraphBL.getActivePipegraphs()
    val etlComponents = pipegraphs.flatMap(pg => pg.etl).filter(etl => etl.isActive)
    logger.info(s"Found ${etlComponents.length} active ETL...")
    etlListSize = etlComponents.length

    val rtComponents = pipegraphs.flatMap(pg => pg.rt).filter(rt => rt.isActive)
    logger.info(s"Found ${rtComponents.length} active RT...")
    //actorListSize = rtComponents.length


    (etlComponents, rtComponents)
  }

}
