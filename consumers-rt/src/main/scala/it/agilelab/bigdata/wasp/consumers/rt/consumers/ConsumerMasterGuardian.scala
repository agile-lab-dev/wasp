package it.agilelab.bigdata.wasp.consumers.rt.consumers

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.WaspMessage
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.cluster.ClusterAwareNodeGuardian
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.{ETLModel, RTModel}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


case object RestartConsumers extends WaspMessage

object ConsumersMasterGuardian {
  val name = "ConsumerMasterGuardian"
}

class ConsumersMasterGuardian(env: {val producerBL: ProducerBL; val pipegraphBL: PipegraphBL;
  val topicBL: TopicBL; val indexBL: IndexBL;
  val rawBL : RawBL; val keyValueBL: KeyValueBL;
  val websocketBL: WebsocketBL; val mlModelBL: MlModelBL;})
  extends ClusterAwareNodeGuardian with Stash {

  val logger = WaspLogger(this.getClass.getName)

  /** STARTUP PHASE **/
  /** *****************/

  context become uninitialized

  /** BASIC METHODS **/
  /** *****************/

  var lastRestartMasterRef: ActorRef = _


  override def preStart(): Unit = {
    super.preStart()
    //TODO:capire joinseednodes
    cluster.joinSeedNodes(Vector(cluster.selfAddress))
  }

  override def initialize(): Unit = {
    logger.info(s"New actor registered with Master!")

    super.initialize()
    lastRestartMasterRef ! true
    context become initialized
    logger.info("ConsumerMasterGuardian Initialized")
    logger.info("Unstashing queued messages...")
    unstashAll()
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
      stopGuardian()
      startGuardian()
  }

  /** PRIVATE METHODS **/
  /** ******************/

  private def stopGuardian() {

    //Stop all actors bound to this guardian and the guardian itself
    logger.info(s"Stopping actors bound to ConsumersMasterGuardian ...")

    //logger.info(s"stopping Spark Context") why was this log line even here? we never stop it!
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, 60 seconds))
    val res = Await.result(globalStatus, 20 seconds)

    if (res reduceLeft (_ && _)) {
      logger.info(s"Graceful shutdown completed.")
    }
    else {
      logger.error(s"Something went wrong! Unable to shutdown all nodes")
    }

  }

  private def startGuardian() {

    logger.info(s"Starting ConsumersMasterGuardian actors...")
    

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
      activeRT.foreach(element => {
        logger.info(s"***Starting RT actor [${element.name}]")
        val rtActor = context.actorOf(Props(new ConsumerRTActor(env, element, self)))
        rtActor ! StartRT
      })
    }
  }

  //TODO: Maybe we should groupBy another field to avoid duplicates (if exist)...
  private def loadActivePipegraphs: (Seq[ETLModel], Seq[RTModel]) = {
    logger.info(s"Loading all active Pipegraphs ...")
    val pipegraphs = env.pipegraphBL.getActivePipegraphs()
    val etlComponents = pipegraphs.flatMap(pg => pg.etl).filter(etl => etl.isActive)
    logger.info(s"Found ${etlComponents.length} active ETL...")

    val rtComponents = pipegraphs.flatMap(pg => pg.rt).filter(rt => rt.isActive)
    logger.info(s"Found ${rtComponents.length} active RT...")
    //actorListSize = rtComponents.length


    (etlComponents, rtComponents)
  }

}
