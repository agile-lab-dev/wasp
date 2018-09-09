package it.agilelab.bigdata.wasp.consumers.spark.batch

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props, Stash}
import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.utils.Quartz2Utils._
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkWriterFactory, SparkWriterFactoryDefault}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.BatchMessages
import it.agilelab.bigdata.wasp.core.models.JobStatus.JobStatus
import it.agilelab.bigdata.wasp.core.models.{BatchJobInstanceModel, BatchJobModel, BatchSchedulerModel, JobStatus}
import it.agilelab.bigdata.wasp.core.utils.SparkBatchConfiguration
import org.apache.commons.lang3.exception.ExceptionUtils
import org.quartz.Scheduler

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


object SparkConsumersBatchMasterGuardian {

  /**
    *
    * Creates exitingWatchdogProps for the [SparkConsumersBatchMasterGuardian] actor
    *
    * @param env The environment
    * @param sparkWriterFactory The spark writer factory
    * @param plugins The plugins
    * @return the exitingWatchdogProps of the [[SparkConsumersBatchMasterGuardian]] actor
    */
  def props(env: {val batchJobBL: BatchJobBL; val batchSchedulersBL: BatchSchedulersBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL}, sparkWriterFactory: SparkWriterFactory, plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin]): Props = {
    props(env.batchJobBL, env.batchSchedulersBL, 10, refFactory => refFactory.actorOf(BatchJobActor.props(env, sparkWriterFactory, plugins, retryDuration)))
  }


  /**
    * Type alias for a function that accepts an [[ActorRefFactory]] and creates an actor.
    */
  type BatchJobActorFactory = ActorRefFactory => ActorRef

  /**
    * quartz2 scheduler for batch jobs.
    */
  val scheduler: Scheduler = buildScheduler()


  /**
    * Creates exitingWatchdogProps for the actor, use this method in tests to plug into the instantiation of children actor via the [[BatchJobActorFactory]] parameter
    * @param batchJobBl The bl to access batch jobs
    * @param batchSchedulersBL The bl to access batch schedulers
    * @param howManySlaveActors How many slave actor to start
    * @param batchJobActorFactory The factory used to instantiate actors
    * @return the exitingWatchdogProps of the [[SparkConsumersBatchMasterGuardian]] actor
    */
  private[batch] def props(batchJobBl: BatchJobBL, batchSchedulersBL: BatchSchedulersBL, howManySlaveActors: Int, batchJobActorFactory: BatchJobActorFactory): Props =
    Props(new SparkConsumersBatchMasterGuardian(batchJobBl, batchSchedulersBL, howManySlaveActors, batchJobActorFactory))


  /**
    * Sent by [[SparkConsumersBatchMasterGuardian]] to itself to perform a round of initialization.
    */
  private[batch] case object Initialize

  /**
    * Sent to [[SparkConsumersBatchMasterGuardian]] in order to request shutdown, it will terminate itself.
    */
  private[batch] case object Terminate

  /**
    * Sent from [[BatchJobActor]] to [[SparkConsumersBatchMasterGuardian]] to request a job.
    */
  private[batch] case object GimmeOneJob

  /**
    * Sent from [[SparkConsumersBatchMasterGuardian]] to assign a Job to the requesting [[BatchJobActor]]
    *
    * @param model    The model of the batch job
    * @param instance The model of the instance
    */
  private[batch] case class Job(model: BatchJobModel, instance: BatchJobInstanceModel)

  /**
    * Sent from [[SparkConsumersBatchMasterGuardian]] to inform the requesting [[BatchJobActor]]
    * that an error occurred while orchestrating the creation and assignment of an instance.
    *
    * @param error The error that occurred.
    */
  private[batch] case class Error(error: Throwable)

  /**
    * Sent from [[SparkConsumersBatchMasterGuardian]] to [[BatchJobActor]] to inform it that no jobs are available
    */
  private[batch] case object NoJobsAvailable

  /**
    * Sent from [[BatchJobActor]] to [[SparkConsumersBatchMasterGuardian]] to inform it that job succeeded
    * @param model THe model of the job that succeeded
    * @param instance The instance of the job that succeeded
    */
  private[batch] case class JobSucceeded(model: BatchJobModel, instance: BatchJobInstanceModel)

  /**
    * Sent from [[BatchJobActor]] to [[SparkConsumersBatchMasterGuardian]] to inform it that a job failed
    * @param model THe model of the job that failed
    * @param instance The instance of the job that failed
    */
  private[batch] case class JobFailed(model: BatchJobModel, instance: BatchJobInstanceModel, error: Throwable)

  /**
    * Default retry times for initialization.
    */
  val retryTimes: Int = 5

  /**
    * Default duration to wait beteen initializations.
    */
  val retryDuration: FiniteDuration = 10.seconds

}


private[batch] trait DatabaseOperations {
  this: SparkConsumersBatchMasterGuardian =>


  /**
    * Creates an instance of the specified model.
    *
    * @param model The model to instantiate
    * @return A Try containing the instance model or an exception
    */
  def createInstanceOf(model: BatchJobModel, restConfig: Config): Try[BatchJobInstanceModel] = Try {

    val instance = BatchJobInstanceModel(
      name = s"${model.name}-${UUID.randomUUID().toString}",
      instanceOf = model.name,
      startTimestamp = System.currentTimeMillis(),
      currentStatusTimestamp = -1,
      status = JobStatus.PENDING,
      restConfig = restConfig
    )

    batchJobBL.instances().insert(instance)
  }

  /**
    * Retrieves the schedulers from the db
    *
    * @return A Try containing the scheduler models or an exception
    */
  def retrieveSchedules(): Try[Seq[BatchSchedulerModel]] = Try {
    batchSchedulerBl.getActiveSchedulers()
  }


  /**
    * Retrieves [[BatchJobModel]]s from the db.
    *
    * @return A Try containing the [[BatchJobModel]]s or an exception
    */
  def retrieveBatchJobs(): Try[Seq[BatchJobModel]] = Try {
    batchJobBL.getAll
  }

  /**
    * Retrieves [[BatchJobModel]]s with specified name from the db.
    *
    * if no batch job exist with name failure is returned
    *
    * @return A Try containing the [[BatchJobModel]]s or an exception
    */
  def retrieveBatchJob(name: String): Try[BatchJobModel] = Try {
    batchJobBL.getByName(name)
  }.flatMap {
    case Some(result) => Success(result)
    case None => Failure(new Exception("Could not retrieve batchJob"))
  }

  /**
    * Retrieves [[BatchJobInstanceModel]]s with specified statuses
    *
    * @param status the status of the [[BatchJobInstanceModel]] that should be retrieved
    * @return A Try containing the [[BatchJobInstanceModel]]s or an exception
    */
  def retrieveBatchJobInstances(status: JobStatus*): Try[Seq[BatchJobInstanceModel]] = Try {
    batchJobBL.instances().all().filter { model => status.toSet.contains(model.status) }
  }

  /**
    * Retrieves [[BatchJobInstanceModel]]s of [[BatchJobModel]] with specified statuses
    *
    * @param job The parent [[BatchJobModel]] whose instances should be retrieved
    * @param status the status of the [[BatchJobInstanceModel]] that should be retrieved
    * @return A Try containing the [[BatchJobInstanceModel]]s or an exception
    */
  def retrieveBatchJobInstances(job: BatchJobModel)(status: JobStatus*): Try[Seq[BatchJobInstanceModel]] = Try {
    batchJobBL.instances().instancesOf(job.name).filter { model => status.toSet.contains(model.status) }
  }

  /**
    * Updates the status of the supplied instances to the target status.
    *
    * @param jobInstances The instances to update
    * @param targetStatus The target status
    * @return A try containing the updated [[BatchJobInstanceModel]] or an exception.
    */
  def updateToStatus(jobInstances: Seq[BatchJobInstanceModel], targetStatus: JobStatus): Try[Seq[BatchJobInstanceModel]] = {
    val result = jobInstances.map(updateToStatus(_, targetStatus))


    val exceptions = result.map {
      case Failure(ex) => Some(ex)
      case Success(_) => None
    }.filter(_.isDefined).map(_.get)

    val models = result.map {
      case Failure(_) => None
      case Success(model) => Some(model)
    }.filter(_.isDefined).map(_.get)


    if (exceptions.nonEmpty) {
      Failure(exceptions.head)
    } else {
      Success(models)
    }

  }

  /**
    * Updates the status of the supplied instance to the target status.
    *
    * @param jobInstance The instance to update
    * @param targetStatus The target status
    * @param maybeError Maybe an error to persist on the database
    * @return A try containing the updated [[BatchJobInstanceModel]] or an exception.
    */
  def updateToStatus(jobInstance: BatchJobInstanceModel, targetStatus: JobStatus, maybeError: Option[Throwable] = None) = Try {

    val updated = maybeError.map { error => ExceptionUtils.getStackTrace(error) }
      .map { error => jobInstance.copy(error = Some(error)) }
      .getOrElse(jobInstance)
      .copy(status = targetStatus, currentStatusTimestamp = System.currentTimeMillis())

    batchJobBL.instances().update(updated)
  }
}


class SparkConsumersBatchMasterGuardian private(val batchJobBL: BatchJobBL,
                                                val batchSchedulerBl: BatchSchedulersBL,
                                                val howManySlaveActors: Int,
                                                jobActorFactory: ActorRefFactory => ActorRef)
  extends Actor
    with Stash
    with SparkBatchConfiguration
    with Logging
    with DatabaseOperations {


  import SparkConsumersBatchMasterGuardian._

  override def preStart() = self ! Initialize

  override def receive: Receive = uninitialized(retryTimes, retryDuration)


  private def startSchedulerActors(schedules: Seq[BatchSchedulerModel]): Unit = {

    if (schedules.isEmpty) {
      logger.info("There are no active batch schedulers")
    } else {
      val sparkConsumersBatchMasterGuardianActorPath = self.path.toStringWithAddress(self.path.address)
      logger.info(s"Found ${schedules.length} batch schedules to be activated")
      schedules foreach {
        schedule => {
          scheduler.scheduleJob(schedule.getQuartzJob(sparkConsumersBatchMasterGuardianActorPath), schedule.getQuartzTrigger)
        }
      }
    }
  }

  /**
    * Behavior of this actor before initialization completes
    *
    * @param retryTimes How many time should retry initialization
    * @param retryDuration The delay between attempts
    * @return The behavior of the actor
    */
  private def uninitialized(retryTimes: Int,
                            retryDuration: FiniteDuration): Receive = {

    case Initialize => {

      val initializationResult = retrieveBatchJobInstances(JobStatus.PENDING, JobStatus.PROCESSING)
        .flatMap(updateToStatus(_, JobStatus.PENDING))
        .flatMap { results => retrieveSchedules().map((results, _)) }


      initializationResult match {
        case Failure(e) => {
          logger.error(s"error in initialization, scheduling retry in $retryDuration", initializationResult.failed.get)

          if (retryTimes > 0) {
            context.become(uninitialized(retryTimes - 1, retryDuration))
            context.system.scheduler.scheduleOnce(retryDuration, self, Initialize)(context.dispatcher)
          } else {
            logger.error("Could not initialize, giving up")
            context.stop(self)
          }
        }
        case Success((pending, schedules)) => {

          val watchedChildren = List.fill(howManySlaveActors) {
            context.watch(jobActorFactory(context))
          }.toSet

          context.become(behavior(pending.toSet, Map.empty, watchedChildren))

          startSchedulerActors(schedules)

          logger.info("initialization complete, unstashing")

          unstashAll()
        }
      }

    }

    case message => {
      logger.info(s"Currently Uninitialized, stashing message [$message]")
      stash()
    }

  }


  /**
    * The main behavior of the actor, composes internal and external behavior.
    *
    * @param pendingJobs The jobs that are currently pending
    * @param runningJobs THe jobs that are currently running associated with the actor that is executing them
    * @param children The children actor that are executing jobs
    * @return The behavior
    */
  private def behavior(pendingJobs: Set[BatchJobInstanceModel],
                       runningJobs: Map[BatchJobInstanceModel, ActorRef],
                       children: Set[ActorRef]): Receive = PartialFunction.empty
    .orElse(internalBehavior(pendingJobs, runningJobs, children))
    .orElse(externalBehavior(pendingJobs, runningJobs, children))


  /**
    * The external behavior of the actor (between the rest of the system and [[SparkConsumersBatchMasterGuardian]].
    *
    * This behavior handles termination and Start/Stop of jobs
    *
    * @param pendingJobs The jobs that are currently pending
    * @param runningJobs THe jobs that are currently running associated with the actor that is executing them
    * @param children The children actor that are executing jobs
    * @return The behavior
    */
  private def externalBehavior(pendingJobs: Set[BatchJobInstanceModel],
                               runningJobs: Map[BatchJobInstanceModel, ActorRef],
                               children: Set[ActorRef]): Receive = {

    case Terminate => {
      context.stop(self)
    }

    case BatchMessages.StartBatchJob(name, restConfig) => {

      val batchJobModel = retrieveBatchJob(name)
      val bjmRetrieved = batchJobModel.isSuccess

      def inConflictWithJobsSet(bjName: String, bjModel: Option[BatchJobModel], jobs: Set[BatchJobInstanceModel]) = {
        // The job is in conflict if the condition " A OR B" holds, where
        // A:  The batch job is fully exclusive and exist already a job instance
        // B:  The batch job is not fully exclusive but exists a job instance with the same exclusive parameters configuration if any
        bjModel.forall {
          bjm =>
            // A
            (
              bjm.exclusivityConfig.isFullyExclusive &&
                jobs.exists(j => j.instanceOf == name)
              ) ||
              // B
              (
                (!bjm.exclusivityConfig.isFullyExclusive) &&
                  bjm.exclusivityConfig.restConfigExclusiveParams.nonEmpty &&
                  jobs.exists { j =>
                    j.instanceOf == name &&
                      bjm.exclusivityConfig.restConfigExclusiveParams.forall {
                        exclusiveParam =>
                          restConfig.getString(exclusiveParam) == j.restConfig.getString(exclusiveParam)
                      }
                  }
                )
        }
      }

      val inConflictWithPendingJobs = inConflictWithJobsSet(name, batchJobModel.toOption, pendingJobs)
      val inConflictWithRunningJobs = inConflictWithJobsSet(name, batchJobModel.toOption, runningJobs.keys.toSet)

      val isInConflict = inConflictWithPendingJobs || inConflictWithRunningJobs

      if (bjmRetrieved && isInConflict) {
        val isFullyExclusive = batchJobModel.get.exclusivityConfig.isFullyExclusive
        val exclusiveParameters = batchJobModel.get.exclusivityConfig.restConfigExclusiveParams.mkString(", ")
        sender() ! BatchMessages.StartBatchJobResultFailure(name,
          s"Cannot start multiple instances of same job [$name]. The batch job is " +
            s"${if(!isFullyExclusive) {
              s"not fully exclusive but have exclusive parameters $exclusiveParameters."
            } else {
              "fully exclusive."
            }
            }"
        )
      } else {

        batchJobModel.flatMap(createInstanceOf(_, restConfig)) match {
          case Success(instance: BatchJobInstanceModel) => {
            context.become(behavior(pendingJobs + instance, runningJobs, children))
            sender() ! BatchMessages.StartBatchJobResultSuccess(name, instance.name)
          }
          case Failure(error) => {
            sender() ! BatchMessages.StartBatchJobResultFailure(name, s"failure creating new batch job instance [${error.getMessage}]")
          }
        }
      }
    }

    case BatchMessages.StopBatchJob(name) => {

      if (!pendingJobs.exists(_.instanceOf == name) && !runningJobs.keys.exists(_.instanceOf == name)) {
        sender() ! BatchMessages.StopBatchJobResultFailure(name, s"Cannot stop job [$name] whose instances are not running or pending")
      } else {
        retrieveBatchJob(name)
          .flatMap(retrieveBatchJobInstances(_)(JobStatus.PENDING, JobStatus.PROCESSING))
          .flatMap { original => updateToStatus(original, JobStatus.STOPPED).map((original, _)) } match {
          case Success((original: Seq[BatchJobInstanceModel], _)) => {

            original.foreach {
              case instance if instance.status == JobStatus.PENDING => context.become(behavior(pendingJobs - instance, runningJobs, children))
              case instance if instance.status == JobStatus.PROCESSING => context.become(behavior(pendingJobs, runningJobs - instance, children))
            }

            sender() ! BatchMessages.StopBatchJobResultSuccess(name)
          }
          case Failure(error) => {
            sender() ! BatchMessages.StopBatchJobResultFailure(name, s"failure stopping instances of job [${error.getMessage}]")
          }
        }
      }
    }
  }


  /**
    * The internal behavior of the actor (between [[SparkConsumersBatchMasterGuardian]] and [[BatchJobActor]].
    *
    * This behavior handles dispatching of jobs and collection of results from slave actors.
    *
    * @param pendingJobs The jobs that are currently pending
    * @param runningJobs THe jobs that are currently running associated with the actor that is executing them
    * @param children The children actor that are executing jobs
    * @return The behavior
    */
  private def internalBehavior(pendingJobs: Set[BatchJobInstanceModel],
                               runningJobs: Map[BatchJobInstanceModel, ActorRef],
                               children: Set[ActorRef]): Receive = {

    case GimmeOneJob if pendingJobs.nonEmpty => {

      val original = pendingJobs.head

      retrieveBatchJob(original.instanceOf).flatMap {
        model =>
          retrieveBatchJobInstances(model)(JobStatus.PENDING).flatMap {
            instances =>
              updateToStatus(instances.head, JobStatus.PROCESSING).map {
                instance => (model, instance)
              }
          }

      } match {

        case Failure(error) => {
          logger.error("error", error)
          sender() ! Error(error)
        }
        case Success((model, updated)) => {

          context become behavior(pendingJobs - original, runningJobs + (updated -> sender()), children)

          sender() ! Job(model, updated)

        }

      }

    }

    case GimmeOneJob if pendingJobs.isEmpty => {
      sender() ! NoJobsAvailable
    }

    case JobSucceeded(model, instance) => {

      val originalSender = sender()

      updateToStatus(instance, JobStatus.SUCCESSFUL) match {
        case Failure(error) => {
          logger.warn(s"Could not update status, retrying in $retryDuration", error)
          context.system.scheduler.scheduleOnce(retryDuration, self, JobSucceeded(model, instance))(context.dispatcher, originalSender)
        }
        case Success(_) => {
          context become behavior(pendingJobs, runningJobs - instance, children)

          originalSender ! "OK"
        }
      }

    }

    case JobFailed(model, instance, error) => {

      val originalSender = sender()

      updateToStatus(instance, JobStatus.FAILED, Some(error)) match {
        case Failure(e) => {
          logger.warn(s"Could not update status, retrying in $retryDuration", e)
          context.system.scheduler.scheduleOnce(retryDuration, self, JobFailed(model, instance, e))(context.dispatcher, originalSender)
        }
        case Success(_) => {
          context become behavior(pendingJobs, runningJobs - instance, children)

          originalSender ! "OK"
        }
      }

    }


  }


}


