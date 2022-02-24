package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.actor.{ActorPath, ActorRef, ActorRefFactory, FSM, LoggingFSM, Props, RootActorPath, Stash}
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.pattern.Patterns.ask
import akka.pattern.PipeToSupport
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReaderFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{ActivationSteps, MaterializationSteps}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Protocol._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.State._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian.ComponentFailedStrategy
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{PipegraphGuardian, Protocol => ChildProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.watchdog.SparkContextWatchDog
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.repository.core.bl._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


class SparkConsumersStreamingMasterGuardian(
                                             protected val pipegraphBL: PipegraphBL,
                                             protected val watchdogCreator: ChildCreator,
                                             collaboratorName: String,
                                             retryInterval: FiniteDuration,
                                             unschedulableCheckInterval: FiniteDuration,
                                             debugActor: Option[ActorRef],
                                             initialSchedulingStrategy: SchedulingStrategy
                                           ) extends FSM[State, Data]
  with LoggingFSM[State, Data]
  with DatabaseOperations
  with Stash
  with Logging
  with PipeToSupport
  with RetrySupport {

  var schedulingStrategy: SchedulingStrategy = initialSchedulingStrategy

  val cluster: Cluster = Cluster(context.system)
  implicit val ec: ExecutionContext = context.dispatcher

  watchdogCreator(self, "spark-context-watchdog", context)

  def retry[F[_] : Recoverable, A](retryable: () => F[A]): A = retry(retryInterval)(retryable)

  def identifyCollaboratorOnMember(m: Member): Future[ActorRef] = {
    context.actorSelection(RootActorPath(m.address) / "user" / collaboratorName).resolveOne(retryInterval)
  }

  def identifyPeerActor(peer: String): Future[ActorRef] = {
    context.actorSelection(ActorPath.fromString(peer)).resolveOne(retryInterval)
  }

  startWith(Idle, NoData)

  when(Idle) {
    case Event(Protocol.Initialize, NoData) =>
      debugActor.foreach(_ ! cluster.selfUniqueAddress)
      cluster.subscribe(self, ClusterEvent.initialStateAsSnapshot, classOf[MemberEvent])
      stay()
    case Event(state: CurrentClusterState, NoData) =>
      val members = state.members
        .filter(m => m.hasRole(WaspSystem.sparkConsumersStreamingMasterGuardianRole))
        .filter(m => m.status == MemberStatus.up)
        .map { m =>
          implicit val ec: ExecutionContext = context.dispatcher

          // TODO: Handle identification in an async way, if a node we are trying to identify
          // TODO: goes down while identifying it we risk to block forever
          val identified = retry(() => identifyCollaboratorOnMember(m))

          Collaborator(m.uniqueAddress, identified, m.roles)
        }

      members.foreach(coll => logger.info(s"MEMBER => $coll"))
      goto(Initializing) using Schedule(Seq(), members) replying Protocol.Initialize

    case Event(msg: Any, _) =>
      log.debug("Stashing current message {}", msg)
      stash
      stay
  }

  when(Initializing, stateTimeout = retryInterval) {
    case Event(StateTimeout | Protocol.Initialize, Schedule(Seq(), members)) =>
      retry(() => resetStatesWhileRecoveringAndReturnPending(members)) match {
        case (processing, pending) =>
          val nextSchedule = pending.foldLeft(Schedule(Seq(), members)) { case (acc, (pipegraph, instance)) =>

            schedulingStrategy.choose(members, pipegraph) match {
              case Right((chosen, nextSchedulingStrategy)) =>
                chosen.collaboratorActor ! WorkAvailable(instance.instanceOf)
                schedulingStrategy = nextSchedulingStrategy
                logger.info(s"scheduling ${pipegraph.name} to ${chosen.address}")
                acc.toPending(self, instance)
              case Left((error, nextSchedulingStrategy)) =>
                logger.error(s"Error while trying to schedule ${pipegraph.name}: ${error}")
                schedulingStrategy = nextSchedulingStrategy
                val updatedInstance = retry(() => updateToStatus(instance.copy(executedByNode = None, peerActor = None), PipegraphStatus.UNSCHEDULABLE))
                acc.toUnschedulable(self, updatedInstance)
            }

          }

          val nextNext = processing.foldLeft(nextSchedule) { case (acc, (_, instance)) =>

            // TODO: Handle identification in an async way, if a node we are trying to identify
            // TODO: goes down while identifying it we risk to block forever
            val peer = retry(() => identifyPeerActor(instance.peerActor.get))

            logger.info(s"Recovered association with $peer")

            acc.toPending(self, instance).toProcessing(peer, instance)
          }
          log.info("Initialization succeeded")
          debugActor.foreach(_ ! SparkConsumersStreamingMasterGuardian.InitializationCompleted)
          goto(Initialized) using nextNext
      }
    case Event(msg: Any, _) =>
      log.debug("Stashing current message {}", msg)
      stash
      stay
  }

  when(Initialized)(compose(handleStart, handleStop, handleWorkerRequest, handleRestart, handleMembership, handleUnschedulable))

  onTransition {
    case (_, Idle) =>
      self ! Protocol.Initialize
    case (Initializing, Initialized) =>
      unstashAll()
      log.debug("Unstashing")
      log.info(s"Setting ${Timers.unschedulableCheck} to recover unshedulable pipegraphs")
      setTimer(Timers.unschedulableCheck, RecoverUnschedulable, unschedulableCheckInterval, repeat = true)
  }

  private def handleUnschedulable: StateFunction = {
    case Event(RecoverUnschedulable, startingSchedule: Schedule) =>
      val finalSchedule = startingSchedule.unschedulable.foldLeft(startingSchedule) {
        case (schedule, ScheduleInstance(worker, pipegraphInstance)) =>

          retrievePipegraph(pipegraphInstance.instanceOf).flatMap(pipegraph => updateToStatus(pipegraphInstance, PipegraphStatus.PENDING).map(instance => (pipegraph, instance))) match {
            case Success((pipegraph, instance)) =>
              schedulingStrategy.choose(schedule.workers, pipegraph) match {
                case Right((chosen, nextSchedulingStrategy)) =>
                  chosen.collaboratorActor ! WorkAvailable(instance.instanceOf)
                  schedulingStrategy = nextSchedulingStrategy
                  logger.info(s"scheduling ${pipegraph.name} to ${chosen.address}")
                  schedule.toPending(self, instance)
                case Left((error, nextSchedulingStrategy)) =>
                  logger.error(s"Error while trying to schedule ${pipegraph.name}: ${error}")
                  schedulingStrategy = nextSchedulingStrategy
                  val updatedInstance = retry(() => updateToStatus(pipegraphInstance, PipegraphStatus.UNSCHEDULABLE))
                  schedule.toUnschedulable(self, updatedInstance)
              }
            case Failure(exception) =>
              logger.error("Cannot retrieve pipegraph, leaving instance as unschedulable", exception)
              schedule.toUnschedulable(self, pipegraphInstance)

          }

      }

      stay using (finalSchedule)
  }

  private def handleStart: StateFunction = {

    case Event(StartSystemPipegraphs, _) =>

      import scala.concurrent.duration._

      retry(() => retrieveSystemPipegraphs()) match {
        case pipegraphs =>
          askToStartSeq(self, pipegraphs.map(_.name), 5.seconds)
            .map(_ => SystemPipegraphsStarted)
            .pipeTo(sender())

      }

      stay

    case Event(StartPipegraph(name), schedule: Schedule) if schedule.doesNotKnow(name) =>
      retrievePipegraph(name).flatMap(pipegraph => createInstanceOf(name).map(instance => (pipegraph, instance))) match {
        case Success((pipegraph, instance)) =>
          val nextSchedule = schedule.toPending(self, instance)
          log.debug(nextSchedule.toString)

          schedulingStrategy.choose(schedule.workers, pipegraph) match {
            case Left((error, nextSchedulingStrategy)) =>
              logger.error(s"Error while trying to schedule ${pipegraph.name}: ${error}")
              val unschedulableInstance = retry(() => updateToStatus(instance, PipegraphStatus.UNSCHEDULABLE))
              val updatedSchedule = nextSchedule.toUnschedulable(self, unschedulableInstance)
              schedulingStrategy = nextSchedulingStrategy
              stay using updatedSchedule replying Protocol.PipegraphStarted(name, instance.name)
            case Right((member, nextSchedulingStrategy)) =>
              log.info("Received start {} sending WorkAvailable to {}", name, member.collaboratorActor)
              member.collaboratorActor ! WorkAvailable(name)
              schedulingStrategy = nextSchedulingStrategy
              stay using nextSchedule replying Protocol.PipegraphStarted(name, instance.name)

          }


        case Failure(error) =>
          stay replying Protocol.PipegraphNotStarted(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StartPipegraph(name), schedule: Schedule) if schedule.knows(name) =>
      stay replying Protocol.PipegraphNotStarted(name, s"Cannot start more than one instance of [$name]")
  }

  private def handleRestart: StateFunction = {

    case Event(RestartConsumers, schedule: Schedule) =>
      import scala.concurrent.duration._

      val toBeRestarted = (schedule.pending ++ schedule.processing).map(_.pipegraphInstance)

      log.info(s"Performing orderly restart of $toBeRestarted")

      orderlyRestart(self, toBeRestarted, 5.seconds)
        .map(_ => ConsumersRestarted)
        .pipeTo(self)

      stay

    case Event(ConsumersRestarted, _) =>
      log.info("Orderly restart finished")
      stay

  }

  private def handleMembership: StateFunction = {

    case Event(MemberLeft(member), _: Schedule) if member.uniqueAddress == cluster.selfUniqueAddress =>
      stop()
    case Event(MemberRemoved(member, _), _: Schedule) if member.uniqueAddress == cluster.selfUniqueAddress =>
      stop()
    case Event(MemberRemoved(member, _), schedule: Schedule) if member.uniqueAddress != cluster.selfUniqueAddress =>
      val lost = schedule.processing(member.uniqueAddress.address)

      lost.foreach(lost => logger.warn(s"LOST => $lost"))

      val newMembers = schedule.workers.filterNot(_.address == member.uniqueAddress)

      val newSchedule = lost.foldLeft(schedule) { (schedule, instance) =>
        self ! StartPipegraph(instance.pipegraphInstance.instanceOf)

        schedule
          .toFailed(instance.worker, instance.pipegraphInstance)
          .copy(workers = newMembers)
      }

      stay using newSchedule

    case Event(MemberUp(member), schedule: Schedule) =>
      val newMembers = if (member.hasRole(WaspSystem.sparkConsumersStreamingMasterGuardianRole)) {

        // TODO: Handle identification in an async way, if a node we are trying to identify
        // TODO: goes down while identifying it we risk to block forever
        schedule.workers + Collaborator(
          member.uniqueAddress,
          retry(() => identifyCollaboratorOnMember(member)),
          member.roles
        )
      } else {
        schedule.workers
      }

      stay() using schedule.copy(workers = newMembers)
  }

  private def handleStop: StateFunction = {
    case Event(StopPipegraph(name), schedule: Schedule) if schedule.canGoToStopped(name) =>
      updateToStatus(
        schedule.stoppable(name).pipegraphInstance.copy(executedByNode = None, peerActor = None),
        PipegraphStatus.STOPPED
      ) match {
        case Success(instance) =>
          val nextSchedule = schedule.toStopped(self, instance)
          stay using nextSchedule replying Protocol.PipegraphStopped(name)

        case Failure(error) =>
          stay replying Protocol.PipegraphNotStopped(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StopPipegraph(name), schedule: Schedule) if schedule.isProcessing(name) =>
      val current = schedule.processing(name)

      updateToStatus(current.pipegraphInstance, PipegraphStatus.STOPPING) match {
        case Success(instance) =>
          current.worker ! ChildProtocol.CancelWork
          val nextSchedule = schedule.toStopping(current.worker, instance)
          stay using nextSchedule replying Protocol.PipegraphStopped(name)

        case Failure(error) =>
          stay replying Protocol.PipegraphNotStopped(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StopPipegraph(name), schedule: Schedule) if !schedule.canGoToStopped(name) =>
      stay replying Protocol.PipegraphNotStopped(name, s"Cannot stop more than one instance of [$name]")
  }

  private def handleWorkerRequest: StateFunction = {
    case Event(ChildProtocol.GimmeWork(member, pipegraphName), schedule: Schedule) =>
      schedule.pending.find(_.instanceOf == pipegraphName) match {
        case Some(ScheduleInstance(worker, pipegraphInstance)) =>
          val inst = pipegraphInstance.copy(executedByNode = Some(formatUniqueAddress(member)), peerActor = Some(sender.path.toString))
          retrievePipegraphAndUpdateInstanceToProcessing(inst) match {
            case Success((model, instance)) =>
              val nextSchedule = schedule.toProcessing(sender(), instance)
              stay using nextSchedule replying Protocol.WorkGiven(model, instance)
            case Failure(failure) =>
              stay replying Protocol.WorkNotGiven(failure)
          }
        case None =>
          stay replying Protocol.WorkNotGiven(new Exception(s"Cannot find ${pipegraphName} instance"))
      }


    case Event(ChildProtocol.WorkCancelled, schedule: Schedule) =>
      val whatWasCancelled = schedule.stopping(sender())

      retry(() =>
        updateToStatus(
          whatWasCancelled.pipegraphInstance.copy(executedByNode = None, peerActor = None),
          PipegraphStatus.STOPPED
        )
      ) match {
        case instance =>
          val nextSchedule = schedule.toStopped(self, instance)
          stay using nextSchedule

      }

    case Event(_: ChildProtocol.WorkNotCancelled, _: Schedule)
    =>
      setTimer(
        Timers.workNotCancelledRetryTimer,
        RetryEnvelope(ChildProtocol.WorkNotCancelled, sender()),
        retryInterval
      )
      stay

    case Event(RetryEnvelope(ChildProtocol.WorkNotCancelled, originalSender), _: Schedule)
    =>
      originalSender ! ChildProtocol.CancelWork
      stay

    case Event(WorkCompleted, schedule: Schedule)
    =>
      val whatCompleted = schedule.stoppingOrProcessing(sender())

      retry(() =>
        updateToStatus(
          whatCompleted.pipegraphInstance.copy(executedByNode = None, peerActor = None),
          PipegraphStatus.STOPPED
        )
      ) match {
        case instance =>
          val nextSchedule = schedule.toStopped(sender(), instance)
          stay using nextSchedule
      }

    case Event(WorkFailed(reason), schedule: Schedule)
    =>
      val whatFailed = schedule.processing(sender())
      retry(() =>
        updateToStatus(
          whatFailed.pipegraphInstance.copy(executedByNode = None, peerActor = None),
          PipegraphStatus.FAILED,
          Some(reason)
        )
      ) match {
        case instance =>
          val nextSchedule = schedule.toFailed(sender(), instance)
          stay using nextSchedule
      }
  }

  initialize()
}

object SparkConsumersStreamingMasterGuardian {

  import scala.concurrent.duration._

  /**
    * A child factory is a function of (pipegraph,actorName, context | Actorsystem)
    */
  type ChildCreator = (ActorRef, String, ActorRefFactory) => ActorRef

  def exitingWatchdogCreator(sc: SparkContext, exitCode: Int): ChildCreator =
    (_, name, context) => context.actorOf(SparkContextWatchDog.exitingWatchdogProps(sc, exitCode), name)

  def doNothingWatchdogCreator(sc: SparkContext): ChildCreator =
    (_, name, context) => context.actorOf(SparkContextWatchDog.logAndDoNothingWatchdogProps(sc), name)

  def defaultChildCreator(
                           sparkSession: SparkSession,
                           sparkReaderFactory: SparkReaderFactory,
                           sparkWriterFactory: SparkWriterFactory,
                           retryDuration: FiniteDuration,
                           monitoringInterval: FiniteDuration,
                           componentFailedStrategy: ComponentFailedStrategy,
                           env: {
                             val pipegraphBL: PipegraphBL
                             val mlModelBL: MlModelBL
                             val indexBL: IndexBL
                             val topicBL: TopicBL
                             val freeCodeBL: FreeCodeBL
                             val processGroupBL: ProcessGroupBL
                             val rawBL: RawBL
                             val keyValueBL: KeyValueBL
                           }
                         ): ChildCreator = { (master, pipegraphName, context) =>
    val name = s"$pipegraphName-${UUID.randomUUID()}"

    val streamingReaderFactory: ActivationSteps.StreamingReaderFactory = {
      (structuredStreamingETLModel, streamingReaderModel, session) =>
        sparkReaderFactory.createSparkStructuredStreamingReader(
          env,
          session,
          structuredStreamingETLModel,
          streamingReaderModel
        )
    }

    val staticReaderFactory: ActivationSteps.StaticReaderFactory = {
      (structuredStreamingETLModel, readerModel, session) =>
        sparkReaderFactory.createSparkBatchReader(env, session.sparkContext, readerModel)
    }

    val writerFactory: MaterializationSteps.WriterFactory = { (structuredStreamingETLModel, writerModel, session) =>
      sparkWriterFactory.createSparkWriterStructuredStreaming(env, session, structuredStreamingETLModel, writerModel)
    }

    val defaultGrandChildrenCreator = PipegraphGuardian.defaultChildFactory(
      sparkSession,
      env.mlModelBL,
      env.topicBL,
      env.freeCodeBL,
      env.processGroupBL,
      streamingReaderFactory,
      staticReaderFactory,
      writerFactory
    )

    //actor names should be urlsafe
    val saneName = URLEncoder.encode(name.replaceAll(" ", "-"), StandardCharsets.UTF_8.name())

    context.actorOf(
      PipegraphGuardian
        .props(master, pipegraphName, defaultGrandChildrenCreator, retryDuration, monitoringInterval, componentFailedStrategy),
      saneName
    )

  }

  def props(pipegraphBl: PipegraphBL, watchDogCreator: ChildCreator, collaboratorName: String, retryInterval: FiniteDuration, unschedulableCheckInterval: FiniteDuration, debugActor: Option[ActorRef] = None, schedulingStrategy: SchedulingStrategyFactory = new FifoSchedulingStrategyFactory): Props =
    Props(new SparkConsumersStreamingMasterGuardian(pipegraphBl, watchDogCreator, collaboratorName, retryInterval,unschedulableCheckInterval, debugActor, schedulingStrategy.create))

  private def compose[A, B](functions: PartialFunction[A, B]*) = functions.foldLeft(PartialFunction.empty[A, B]) {
    (acc, elem) =>
      acc.orElse(elem)
  }

  private def sequenceFutures[T, U](
                                     xs: TraversableOnce[T]
                                   )(f: T => Future[U])(implicit context: ExecutionContext): Future[List[U]] = {
    val resBase = Future.successful(mutable.ListBuffer.empty[U])
    xs.foldLeft(resBase) { (futureRes, x) =>
      futureRes.flatMap { res =>
        f(x).map(res += _)
      }
    }
      .map(_.toList)
  }

  private def askToStop(ref: ActorRef, pipegraph: String, timeout: FiniteDuration)(
    implicit context: ExecutionContext
  ): Future[String] =
    ask(ref, StopPipegraph(pipegraph), timeout).flatMap {
      case PipegraphStopped(`pipegraph`) => Future.successful(pipegraph)
      case PipegraphNotStopped(`pipegraph`, _) => askToStop(ref, pipegraph, timeout)
      case _ => throw new Exception("unexpected result")
    }

  private def askToStart(ref: ActorRef, pipegraph: String, timeout: FiniteDuration)(
    implicit context: ExecutionContext
  ): Future[String] =
    ask(ref, StartPipegraph(pipegraph), timeout).flatMap {
      case PipegraphStarted(`pipegraph`, _) => Future.successful(pipegraph)
      case PipegraphNotStarted(`pipegraph`, _) => askToStart(ref, pipegraph, timeout)
      case _ => throw new Exception("unexpected result")
    }

  private def askToStopSeq(ref: ActorRef, pipegraphs: Seq[String], timeout: FiniteDuration)(
    implicit context: ExecutionContext
  ): Future[Seq[String]] =
    sequenceFutures(pipegraphs)(askToStop(ref, _, timeout))

  private def askToStartSeq(ref: ActorRef, pipegraphs: Seq[String], timeout: FiniteDuration)(
    implicit context: ExecutionContext
  ): Future[Seq[String]] =
    sequenceFutures(pipegraphs)(askToStart(ref, _, timeout))

  private def orderlyRestart(guardian: ActorRef, pipegraphs: Seq[PipegraphInstanceModel], timeout: FiniteDuration)(
    implicit context: ExecutionContext
  ): Future[Unit] = {

    askToStopSeq(guardian, pipegraphs.map(_.instanceOf), timeout)
      .flatMap(askToStartSeq(guardian, _, timeout))
      .map(_ => Unit)
  }

  case class RetryEnvelope[O](original: O, sender: ActorRef)

  object InitializationCompleted

  object RecoverUnschedulable

  object Timers {
    val workFailedRetryTimer = "work-failed-retry-timer"
    val workNotCancelledRetryTimer = "work-not-cancelled-retry-timer"
    val cancelWorkRetryTimer = "cancel-work-retry-timer"
    val workCompleted = "completed-retry-timer"
    val unschedulableCheck = "unschedulable-check"
  }

  def formatUniqueAddress(address: UniqueAddress) =
    s"${address.address.toString}/${address.longUid}"

}
