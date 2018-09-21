package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.actor.{ActorRef, ActorRefFactory, FSM, Props, Stash}
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
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class SparkConsumersStreamingMasterGuardian(protected val pipegraphBL: PipegraphBL,
                                            protected val childCreator: ChildCreator,
                                            protected val watchdogCreator: ChildCreator,
                                            retryInterval: FiniteDuration)
  extends FSM[State, Data]
    with DatabaseOperations
    with Stash
    with Logging
    with PipeToSupport {

  watchdogCreator(self, "spark-context-watchdog", context)

  startWith(Idle, NoData)

  when(Idle) {
    case Event(Protocol.Initialize, NoData) =>
      goto(Initializing) using Schedule(Seq()) replying Protocol.Initialize

  }

  when(Initializing, stateTimeout = retryInterval) {
    case Event(StateTimeout | Protocol.Initialize, Schedule(Seq())) =>
      resetStatesWhileRecoveringAndReturnPending match {
        case Success(pending) =>
          val nextSchedule = pending.foldLeft(Schedule(Seq())) { (acc, instance) => acc.toPending(self, instance) }
          val child = pending.map(pending => childCreator(self, pending.name, context))
          child.foreach(_ ! WorkAvailable)
          log.info("Initialization succeeded")
          goto(Initialized) using nextSchedule
        case Failure(e) =>
          log.warning("Initialization failed, retrying [{}]", e.getMessage)
          goto(Initializing)
      }
  }

  when(Initialized)(compose(handleStart, handleStop, handleWorkerRequest, handleRestart))

  whenUnhandled {
    case Event(msg: Any, _) =>
      log.debug("Stashing current message {}", msg)
      stash
      stay
  }


  onTransition {
    case (_, Idle) =>
      self ! Protocol.Initialize
    case (Initializing, Initialized) =>
      unstashAll()
  }

  private def handleStart: StateFunction = {

    case Event(StartSystemPipegraphs, _) => {

      import scala.concurrent.duration._

      implicit val executionContext: ExecutionContext = context.dispatcher

      retrieveSystemPipegraphs() match {
        case Success(pipegraphs) => askToStartSeq(self,pipegraphs.map(_.name),5.seconds)
          .map(_ => SystemPipegraphsStarted ).pipeTo(sender())

        case Failure(reason) => self.forward(StartSystemPipegraphs)
      }

      stay
    }

    case Event(StartPipegraph(name), schedule: Schedule) if schedule.doesNotKnow(name) =>

      createInstanceOf(name) match {
        case Success(instance) =>

          val nextSchedule = schedule.toPending(self, instance)
          val child = childCreator(self, instance.name, context)
          child ! WorkAvailable

          stay using nextSchedule replying Protocol.PipegraphStarted(name, instance.name)

        case Failure(error) =>
          stay replying Protocol.PipegraphNotStarted(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StartPipegraph(name), schedule: Schedule) if schedule.knows(name) =>
      stay replying Protocol.PipegraphNotStarted(name, s"Cannot start more than one instance of [$name]")
  }


  private def handleRestart: StateFunction = {

    case Event(RestartConsumers, schedule: Schedule) =>
      import scala.concurrent.duration._

      implicit val executionContext: ExecutionContext = context.dispatcher

      val toBeRestarted = (schedule.pending ++ schedule.processing).map(_.pipegraphInstance)

      log.info(s"Performing orderly restart of $toBeRestarted")

      orderlyRestart(self, toBeRestarted, 5.seconds).map(_ => ConsumersRestarted)
                                                    .pipeTo(self)

      stay

    case Event(ConsumersRestarted, _) =>
      log.info("Orderly restart finished")
      stay


  }

  private def handleStop: StateFunction = {
    case Event(StopPipegraph(name), schedule: Schedule) if schedule.isPending(name) =>
      updateToStatus(schedule.pending(name).pipegraphInstance, PipegraphStatus.STOPPED) match {
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
          val nextSchedule = schedule.toStopping(current.worker, instance)
          current.worker ! ChildProtocol.CancelWork
          stay using nextSchedule replying Protocol.PipegraphStopped(name)

        case Failure(error) =>
          stay replying Protocol.PipegraphNotStopped(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StopPipegraph(name), schedule: Schedule)
      if !schedule.canGoToStopped(name) =>
      stay replying Protocol.PipegraphNotStopped(name, s"Cannot stop more than one instance of [$name]")
  }


  private def handleWorkerRequest: StateFunction = {
    case Event(ChildProtocol.GimmeWork, schedule: Schedule) =>
      retrievePipegraphAndUpdateInstanceToProcessing(schedule.pending.head.pipegraphInstance) match {
        case Success((model, instance)) =>
          val nextSchedule = schedule.toProcessing(sender(), instance)
          stay using nextSchedule replying Protocol.WorkGiven(model, instance)
        case Failure(failure) =>
          stay replying Protocol.WorkNotGiven(failure)
      }

    case Event(ChildProtocol.WorkCancelled, _: Schedule) =>
      self ! RetryEnvelope(ChildProtocol.WorkCancelled, sender())
      stay

    case Event(RetryEnvelope(ChildProtocol.WorkCancelled, originalSender), schedule: Schedule) =>
      val whatWasCancelled = schedule.stopping(originalSender)
      updateToStatus(whatWasCancelled.pipegraphInstance, PipegraphStatus.STOPPED) match {
        case Success(instance) =>
          val nextSchedule = schedule.toStopped(self, instance)
          stay using nextSchedule
        case Failure(_) =>
          setTimer(Timers.cancelWorkRetryTimer, RetryEnvelope(ChildProtocol.WorkCancelled, originalSender), retryInterval)
          stay
      }

    case Event(_: ChildProtocol.WorkNotCancelled, _: Schedule) =>
      setTimer(Timers.workNotCancelledRetryTimer, RetryEnvelope(ChildProtocol.WorkNotCancelled, sender()), retryInterval)
      stay

    case Event(RetryEnvelope(ChildProtocol.WorkNotCancelled, originalSender), schedule: Schedule) =>
      originalSender ! ChildProtocol.CancelWork
      stay


    case Event(WorkCompleted, _: Schedule) =>
      self ! RetryEnvelope(WorkCompleted, sender())
      stay

    case Event(RetryEnvelope(WorkCompleted, originalSender), schedule: Schedule) =>

      val whatCompleted = schedule.stoppingOrProcessing(originalSender)

      updateToStatus(whatCompleted.pipegraphInstance, PipegraphStatus.STOPPED) match {
        case Success(instance) =>
          val nextSchedule = schedule.toFailed(originalSender, instance)
          stay using nextSchedule
        case Failure(_) =>
          setTimer(Timers.workCompleted, RetryEnvelope(WorkCompleted, originalSender), retryInterval)
          stay
      }

    case Event(message: WorkFailed, _: Schedule) =>
      self ! RetryEnvelope(message, sender())
      stay

    case Event(RetryEnvelope(message@WorkFailed(reason), originalSender), schedule: Schedule) =>
      val whatFailed = schedule.processing(originalSender)
      updateToStatus(whatFailed.pipegraphInstance, PipegraphStatus.FAILED, Some(reason)) match {
        case Success(instance) =>
          val nextSchedule = schedule.toFailed(originalSender, instance)
          stay using nextSchedule
        case Failure(_) =>
          setTimer(Timers.workFailedRetryTimer, RetryEnvelope(message, originalSender), retryInterval)
          stay
      }
  }

  initialize()
}

object SparkConsumersStreamingMasterGuardian  {

  import scala.concurrent.duration._

  /**
    * A child factory is a function of (pipegraph,actorName, context | Actorsystem)
    */
  type ChildCreator = (ActorRef, String, ActorRefFactory) => ActorRef


  def exitingWatchdogCreator(sc: SparkContext, exitCode: Int): ChildCreator = (_, name, context) =>
    context.actorOf(SparkContextWatchDog.exitingWatchdogProps(sc, exitCode), name)

  def doNothingWatchdogCreator(sc: SparkContext): ChildCreator = (_, name, context) =>
    context.actorOf(SparkContextWatchDog.logAndDoNothingWatchdogProps(sc), name)

  def defaultChildCreator(sparkSession: SparkSession,
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
                            val rawBL: RawBL
                            val keyValueBL: KeyValueBL
                          }): ChildCreator = { (master, suppliedName, context) =>


    val name = s"$suppliedName-${UUID.randomUUID()}"
  
    val streamingReaderFactory: ActivationSteps.StreamingReaderFactory = { (structuredStreamingETLModel, streamingReaderModel) =>
      sparkReaderFactory.createSparkStructuredStreamingReader(env, sparkSession, structuredStreamingETLModel, streamingReaderModel)
    }
  
    val staticReaderFactory: ActivationSteps.StaticReaderFactory = { (structuredStreamingETLModel, readerModel) =>
      sparkReaderFactory.createSparkBatchReader(env, sparkSession.sparkContext, readerModel)
    }
    
    val writerFactory: MaterializationSteps.WriterFactory = { (structuredStreamingETLModel, writerModel) =>
      sparkWriterFactory.createSparkWriterStructuredStreaming(env, sparkSession, structuredStreamingETLModel, writerModel)
    }

    val defaultGrandChildrenCreator = PipegraphGuardian.defaultChildFactory(sparkSession,
                                                                            env.mlModelBL,
                                                                            env.topicBL,
                                                                            streamingReaderFactory,
                                                                            staticReaderFactory,
                                                                            writerFactory)

    //actor names should be urlsafe
    val saneName = URLEncoder.encode(name.replaceAll(" ", "-"), StandardCharsets.UTF_8.name())

    context.actorOf(PipegraphGuardian.props(master, defaultGrandChildrenCreator, retryDuration, monitoringInterval,
      componentFailedStrategy), saneName)

  }


  def props(pipegraphBl: PipegraphBL, childCreator: ChildCreator,watchDogCreator: ChildCreator,
            retryInterval: FiniteDuration): Props =
    Props(new SparkConsumersStreamingMasterGuardian(pipegraphBl, childCreator, watchDogCreator, retryInterval))
  private def compose[A, B](functions: PartialFunction[A, B]*) = functions.foldLeft(PartialFunction.empty[A, B]) {
    (acc, elem) => acc.orElse(elem)
  }


  private def sequenceFutures[T, U](xs: TraversableOnce[T])(f: T => Future[U])(implicit context:ExecutionContext): Future[List[U]] = {
    val resBase = Future.successful(mutable.ListBuffer.empty[U])
    xs.foldLeft(resBase) { (futureRes, x) =>
      futureRes.flatMap {
        res => f(x).map(res += _)
      }
    }.map(_.toList)
  }

  private def askToStop(ref: ActorRef, pipegraph:String, timeout:FiniteDuration)(implicit context:ExecutionContext): Future[String] =
    ask(ref, StopPipegraph(pipegraph), timeout).flatMap {
      case PipegraphStopped(`pipegraph`) => Future.successful(pipegraph)
      case PipegraphNotStopped(`pipegraph`, _) => askToStop(ref, pipegraph,timeout)
      case _ => throw new Exception("unexpected result")
    }

  private def askToStart(ref: ActorRef, pipegraph:String, timeout:FiniteDuration)(implicit context:ExecutionContext) : Future[String] =
    ask(ref, StartPipegraph(pipegraph), timeout).flatMap {
      case PipegraphStarted(`pipegraph`, _) => Future.successful(pipegraph)
      case PipegraphNotStarted(`pipegraph`, _) => askToStart(ref, pipegraph,timeout)
      case _ => throw new Exception("unexpected result")
    }


  private def askToStopSeq(ref:ActorRef, pipegraphs: Seq[String], timeout:FiniteDuration)(implicit context:ExecutionContext): Future[Seq[String]] =
    sequenceFutures(pipegraphs)(askToStop(ref,_, timeout))


  private def askToStartSeq(ref:ActorRef, pipegraphs: Seq[String], timeout:FiniteDuration)(implicit context:ExecutionContext): Future[Seq[String]] =
    sequenceFutures(pipegraphs)(askToStart(ref, _,timeout))

  private def orderlyRestart(guardian: ActorRef, pipegraphs : Seq[PipegraphInstanceModel], timeout:FiniteDuration)
                    (implicit context:ExecutionContext): Future[Unit] = {

    askToStopSeq(guardian, pipegraphs.map(_.instanceOf), timeout).flatMap(askToStartSeq(guardian, _, timeout)).map(_ =>
      Unit)
  }



  case class RetryEnvelope[O](original: O, sender: ActorRef)

  object Timers {
    val workFailedRetryTimer = "work-failed-retry-timer"
    val workNotCancelledRetryTimer = "work-not-cancelled-retry-timer"
    val cancelWorkRetryTimer = "cancel-work-retry-timer"
    val workCompleted = "completed-retry-timer"

  }

}
