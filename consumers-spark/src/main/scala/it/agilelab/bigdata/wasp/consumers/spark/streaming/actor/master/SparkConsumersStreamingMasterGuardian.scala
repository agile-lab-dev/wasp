package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.{ActorRef, ActorRefFactory, FSM, Props, Stash}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StructuredStreamingReader
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Protocol._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.State._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian.ComponentFailedStrategy
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{PipegraphGuardian, Protocol => ChildProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import it.agilelab.bigdata.wasp.core
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class SparkConsumersStreamingMasterGuardian(protected val pipegraphBL: PipegraphBL,
                                            protected val childCreator: ChildCreator,
                                            retryInterval: FiniteDuration)
  extends FSM[State, Data]
    with DatabaseOperations
    with Stash
    with Logging {


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
          val child = pending.map(_ => childCreator(self,context))
          child.foreach(_ ! WorkAvailable)
          log.info("Initialization succeeded")
          goto(Initialized) using nextSchedule
        case Failure(e) =>
          log.warning("Initialization failed, retrying [{}]", e.getMessage)
          goto(Initializing)
      }
  }

  when(Initialized)(compose(handleStart, handleStop, handleWorkerRequest))

  whenUnhandled {
    case Event(msg: Protocol, _) =>
      log.debug("Stashing current message {}", msg)
      stash
      stay
    case Event(msg, _) =>
      log.warning("Received unknown message dropping it! [{}]", msg)
      stay
  }


  onTransition {
    case (_, Idle) =>
      self ! Protocol.Initialize
    case (Initializing, Initialized) =>
      unstashAll()
  }


  private def handleStart: StateFunction = {
    case Event(StartPipegraph(name), schedule: Schedule) if schedule.doesNotKnow(name) =>

      createInstanceOf(name) match {
        case Success(instance) =>

          val nextSchedule = schedule.toPending(self, instance)
          val child = childCreator(self, context)
          child ! WorkAvailable

          stay using nextSchedule replying Protocol.PipegraphStarted(name)

        case Failure(error) =>
          stay replying Protocol.PipegraphNotStarted(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StartPipegraph(name), schedule: Schedule) if schedule.knows(name) =>
      stay replying Protocol.PipegraphNotStarted(name, s"Cannot start more than one instance of [$name]")
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

    case Event(_:ChildProtocol.WorkNotCancelled, _: Schedule) =>
      setTimer(Timers.workNotCancelledRetryTimer, RetryEnvelope(ChildProtocol.WorkNotCancelled, sender()), retryInterval)
      stay

    case Event(RetryEnvelope(ChildProtocol.WorkNotCancelled, originalSender), schedule: Schedule) =>
      println("sent")
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

    case Event(message:WorkFailed, _: Schedule) =>
      self ! RetryEnvelope(message,sender())
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

object SparkConsumersStreamingMasterGuardian {

  import scala.concurrent.duration._

  type ChildCreator = (ActorRef, ActorRefFactory) => ActorRef


  def defaultChildCreator(reader: StructuredStreamingReader,
                          plugins: Map[String, WaspConsumersSparkPlugin],
                          sparkSession: SparkSession,
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
                          }): ChildCreator = { (master, context) =>



    val writerFactory : MaterializationSteps.WriterFactory = { writerModel =>
      sparkWriterFactory.createSparkWriterStructuredStreaming(env,sparkSession,writerModel)
    }


    val defaultGrandChildrenCreator = PipegraphGuardian.defaultChildFactory(reader, plugins, sparkSession,env.mlModelBL,
      env.topicBL,writerFactory)



    context.actorOf(PipegraphGuardian.props(master,defaultGrandChildrenCreator,retryDuration,monitoringInterval,componentFailedStrategy))

  }

  def props(pipegraphBl: PipegraphBL, childCreator: ChildCreator, retryInterval: FiniteDuration): Props =
    Props(new SparkConsumersStreamingMasterGuardian(pipegraphBl, childCreator, retryInterval))

  private def compose[A, B](functions: PartialFunction[A, B]*) = functions.foldLeft(PartialFunction.empty[A, B]) {
    (acc, elem) => acc.orElse(elem)
  }


  object Timers {
    val workFailedRetryTimer = "work-failed-retry-timer"
    val workNotCancelledRetryTimer = "work-not-cancelled-retry-timer"
    val cancelWorkRetryTimer = "cancel-work-retry-timer"
    val workCompleted = "completed-retry-timer"

  }

  case class RetryEnvelope[O](original:O, sender:ActorRef)

}
