package it.agilelab.bigdata.wasp.consumers.spark.batch

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.pattern.gracefulStop
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.utils.Quartz2Utils._
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages._
import it.agilelab.bigdata.wasp.core.models.{BatchJobModel, BatchSchedulerModel, JobStateEnum}
import it.agilelab.bigdata.wasp.core.utils.SparkBatchConfiguration
import org.mongodb.scala.bson.BsonObjectId
import org.quartz.Scheduler

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object SparkConsumersBatchMasterGuardian {
  // quartz2 scheduler for batch jobs
  val scheduler: Scheduler = buildScheduler()
}

class SparkConsumersBatchMasterGuardian(env: {
                                          val batchJobBL: BatchJobBL
                                          val indexBL: IndexBL
                                          val rawBL: RawBL
                                          val keyValueBL: KeyValueBL
                                          val mlModelBL: MlModelBL
                                          val batchSchedulerBL: BatchSchedulersBL
                                        },
                                        classLoader: Option[ClassLoader] = None,
                                        sparkWriterFactory: SparkWriterFactory,
                                        plugins: Map[String, WaspConsumersSparkPlugin])
  extends Actor
    with Stash
    with SparkBatchConfiguration
    with Logging {

  import SparkConsumersBatchMasterGuardian._

  var batchActor: ActorRef =_

  /** BASIC METHODS **/
  /** *****************/
  var lastRestartMasterRef: ActorRef = _

  // standard receive
  // NOTE: THIS IS IMMEDIATELY SWITCHED TO uninitialized DURING preStart(), DO NOT USE!
  override def receive: Actor.Receive = uninitialized

  override def preStart(): Unit = {
    // we start in uninitialized state
    context become uninitialized

    // initialize Spark
    val scCreated = SparkSingletons.initializeSpark(sparkBatchConfig)
    if (!scCreated) logger.warn("The spark context was already intialized: it might not be using the spark batch configuration!")

    val sc = SparkSingletons.getSparkContext
    batchActor = context.actorOf(Props(new BatchJobActor(env, classLoader, sparkWriterFactory, sc, plugins)))
  }

  def initialize(): Unit = {
    //no initialization actually, clean code
    context become initialized
    logger.info("SparkConsumersBatchMasterGuardian Initialized")
    unstashAll()
  }

  def uninitialized: Actor.Receive = {
    case message: StopBatchJobsMessage =>
      lastRestartMasterRef = sender()

    //TODO: logica di qualche tipo?
    case message: CheckJobsBucketMessage =>
      lastRestartMasterRef = sender()
      stash()
      initialize()

    case message: StartBatchJobMessage =>
      lastRestartMasterRef = sender()
      stash()
      initialize()

    case message: BatchJobProcessedMessage =>
      stash()
      initialize()

    case message: StartSchedulersMessage =>
      stash()
      initialize()
  }

  def initialized: Actor.Receive = {
    case message: StopBatchJobsMessage =>
      lastRestartMasterRef = sender()
      stopGuardian()

    case message: CheckJobsBucketMessage =>
      lastRestartMasterRef = sender()
      logger.info("Checking batch jobs bucket...")
      checkJobsBucket()

    case message: StartBatchJobMessage =>
      lastRestartMasterRef = sender()
      logger.info(s"Processing batch job ${message.id}")
      lastRestartMasterRef ! BatchJobResult(message.id, startJob(message.id))

    case message: BatchJobProcessedMessage =>
      logger.info(s"Batch job ${message.id} processed with result ${message.jobState}")
      lastRestartMasterRef ! BatchJobProcessedMessage

    case message: StartSchedulersMessage =>
      logger.info("Starting scheduled batches activity")
      startSchedulerActors()
  }

  /** PRIVATE METHODS **/
  /** ******************/

  private def stopGuardian() {
    //Stop all actors bound to this guardian and the guardian itself
    logger.info("Stopping actors bound to SparkConsumersBatchMasterGuardian...")
    val globalStatus = Future.traverse(context.children)(gracefulStop(_, 60 seconds))
    val res = Await.result(globalStatus, 20 seconds)

    if (res reduceLeft (_ && _)) {
      logger.info("Graceful shutdown completed.")
    }
    else {
      logger.error("Something went wrong! Unable to shutdown all nodes")
    }
  }

  private def checkJobsBucket() {
    val batchJobs = loadBatchJobs

    if (batchJobs.isEmpty) {
      logger.info("There are no new pending batch jobs")
      lastRestartMasterRef ! true
    } else {
      batchJobs.foreach(element => {
        logger.info(s"***Starting Batch job actor [${element.name}]")
        context.children.foreach( child => {
          child ! element
        })
      })
      logger.info("Pending jobs sent to BatchJobsActor")
    }
  }

  private def startSchedulerActors(): Unit = {
    val schedules = loadSchedules

    if (schedules.isEmpty) {
      logger.info("There are no active batch schedulers")
    } else {
      val sparkConsumersBatchMasterGuardianActorPath = self.path.toStringWithAddress(self.path.address)
      logger.info(s"Found ${schedules.length} batch schedules to be activated")
      //TODO salvo una lista degli scheduler per gestioni successive? (e.g. stop scheduling...?)
      schedules foreach {
        schedule => {
          scheduler.scheduleJob(schedule.getQuartzJob(sparkConsumersBatchMasterGuardianActorPath), schedule.getQuartzTrigger)
        }
      }
    }
  }

  private def startJob(name: String): Boolean = {
    val job: Option[BatchJobModel] = env.batchJobBL.getByName(name)
    logger.info(s"Job that will be processed, job: $job")
    job match {
      case Some(element) =>
        if (!element.state.equals(JobStateEnum.PROCESSING)) {
          changeBatchState(element.name, JobStateEnum.PENDING)
          batchActor ! element
          true
        } else {
          logger.error(s"Batch job ${element.name} is already in processing phase")
          false
        }
      case None => logger.error("BatchEndedMessage with invalid name found.")
        false
    }
  }

  private def loadBatchJobs: Seq[BatchJobModel] = {
    logger.info("Loading all batch jobs ...")
    val batchJobEntries  = env.batchJobBL.getPendingJobs()
    logger.info(s"Found ${batchJobEntries.length} pending batch jobs...")

    batchJobEntries
  }

  private def loadSchedules: Seq[BatchSchedulerModel] = {
    logger.info("Loading all batch schedules ...")
    val schedules  = env.batchSchedulerBL.getActiveSchedulers()
    logger.info(s"Found ${schedules.length} active schedules...")

    schedules
  }

  private def changeBatchState(name: String, newState: String): Unit =
  {
    val job = env.batchJobBL.getByName(name)
    job match {
      case Some(jobModel) => env.batchJobBL.setJobState(jobModel, newState)
      case None => logger.error("BatchEndedMessage with invalid name found.")
    }
  }
}