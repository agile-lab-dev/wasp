package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.guardian.master

import akka.actor.{ActorRef, ActorRefFactory, FSM, Props, Stash}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

import SparkConsumersStreamingMasterGuardian._
import Protocol._
import State._
import Data._

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
          val child = pending.map(_ => childCreator(context))
          child.foreach(context.watch)
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
          val child = childCreator(context)
          context.watch(child)
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
          current.worker ! CancelWork
          stay using nextSchedule replying Protocol.PipegraphStopped(name)

        case Failure(error) =>
          stay replying Protocol.PipegraphNotStopped(name, ExceptionUtils.getStackTrace(error))
      }

    case Event(StopPipegraph(name), schedule: Schedule)
      if !schedule.canGoToStopped(name) =>
      stay replying Protocol.PipegraphNotStopped(name, s"Cannot stop more than one instance of [$name]")
  }


  private def handleWorkerRequest: StateFunction = {
    case Event(GimmeWork, schedule: Schedule) =>
      retrievePipegraphAndUpdateInstanceToProcessing(schedule.pending.head.pipegraphInstance) match {
        case Success((model, instance)) =>
          val nextSchedule = schedule.toProcessing(sender(), instance)
          stay using nextSchedule replying Protocol.WorkGiven(model, instance)
        case Failure(failure) =>
          stay replying Protocol.WorkNotGiven(failure)
      }

    case Event(WorkCancelled, _: Schedule) =>
      self ! RetryEnvelope(WorkCancelled, sender())
      stay

    case Event(RetryEnvelope(WorkCancelled, originalSender), schedule: Schedule) =>
      val whatWasCancelled = schedule.stopping(originalSender)
      updateToStatus(whatWasCancelled.pipegraphInstance, PipegraphStatus.STOPPED) match {
        case Success(instance) =>
          val nextSchedule = schedule.toStopped(self, instance)
          stay using nextSchedule
        case Failure(_) =>
          setTimer(Timers.cancelWorkRetryTimer, RetryEnvelope(WorkCancelled, originalSender), retryInterval)
          stay
      }

    case Event(_:WorkNotCancelled, _: Schedule) =>
      setTimer(Timers.workNotCancelledRetryTimer, RetryEnvelope(WorkNotCancelled, sender()), retryInterval)
      stay

    case Event(RetryEnvelope(WorkNotCancelled, originalSender), schedule: Schedule) =>
      println("sent")
      originalSender ! CancelWork
      stay


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

  type ChildCreator = (ActorRefFactory) => ActorRef

  def props(pipegraphBL: PipegraphBL): Props = props(pipegraphBL, ???, 5.seconds)

  private[streaming] def props(pipegraphBl: PipegraphBL, childCreator: ChildCreator, retryInterval: FiniteDuration): Props =
    Props(new SparkConsumersStreamingMasterGuardian(pipegraphBl, childCreator, retryInterval))

  private def compose[A, B](functions: PartialFunction[A, B]*) = functions.foldLeft(PartialFunction.empty[A, B]) {
    (acc, elem) => acc.orElse(elem)
  }


  object Timers {
    val workFailedRetryTimer = "work-failed-retry-timer"
    val workNotCancelledRetryTimer = "work-not-cancelled-retry-timer"
    val cancelWorkRetryTimer = "cancel-work-retry-timer"
  }

}
