package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.{ActorRef, ActorSelection, Address}
import akka.cluster.UniqueAddress
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphStatus}

import scala.collection.immutable.Queue

/**
  * Trait marking classes holding [[SparkConsumersStreamingMasterGuardian]] State Data
  */
sealed trait Data

object Data {

  /**
    * Case class representing an element of the current schedule, it associates a worker to a pipegraph instance
    * @param worker The worker
    * @param pipegraphInstance The pipegraph instance
    */
  case class ScheduleInstance(worker: ActorRef, pipegraphInstance: PipegraphInstanceModel) {
    def instanceOf: String = pipegraphInstance.instanceOf
  }

  /**
    * Empty state data.
    */
  case object NoData extends Data


  case class Collaborator(address: UniqueAddress, actorSelection: ActorRef)

  /**
    * Data of the [[State.Initialized]] state
    *
    * @param scheduleInstances The current know schedules to be instantiated
    */
  case class Schedule private (scheduleInstances: Seq[ScheduleInstance], workers: Queue[Collaborator]) extends Data {

    private val byStatus = scheduleInstances
      .groupBy(instance => instance.pipegraphInstance.status)
      .mapValues(value => value.map(_.instanceOf).toSet)

    private val known = byStatus.values.flatten.toSet

    def toPending(worker: ActorRef, instance: PipegraphInstanceModel): Schedule =
      moveTo(worker, instance.copy(peerActor = None, executedByNode = None), PipegraphStatus.PENDING)

    def toStopping(worker: ActorRef, instance: PipegraphInstanceModel): Schedule =
      moveTo(worker, instance, PipegraphStatus.STOPPING)

    def toStopped(worker: ActorRef, instance: PipegraphInstanceModel): Schedule =
      moveTo(worker, instance, PipegraphStatus.STOPPED)

    def toProcessing(worker: ActorRef, instance: PipegraphInstanceModel): Schedule =
      moveTo(worker, instance, PipegraphStatus.PROCESSING)

    def toFailed(worker: ActorRef, instance: PipegraphInstanceModel): Schedule =
      moveTo(worker, instance, PipegraphStatus.FAILED)

    private def moveTo(worker: ActorRef, instance: PipegraphInstanceModel, status: PipegraphStatus): Schedule = {

      val updatedInstance = instance.copy(currentStatusTimestamp = System.currentTimeMillis(), status = status)

      val updatedScheduleInstances = scheduleInstances.filterNot(_.instanceOf == instance.instanceOf) :+
        ScheduleInstance(worker, updatedInstance)

      val statusesToForget = Set(PipegraphStatus.STOPPED, PipegraphStatus.FAILED)

      Schedule(
        updatedScheduleInstances.filterNot(instance => statusesToForget.contains(instance.pipegraphInstance.status)),
        workers
      )
    }

    def pending: Seq[ScheduleInstance] = scheduleInstances.filter(_.pipegraphInstance.status == PipegraphStatus.PENDING)
    def processing: Seq[ScheduleInstance] =
      scheduleInstances.filter(_.pipegraphInstance.status == PipegraphStatus.PROCESSING)

    def pending(instanceOf: String): ScheduleInstance = byStatus(instanceOf, PipegraphStatus.PENDING)

    def stopping(worker: ActorRef): ScheduleInstance = byStatus(worker, PipegraphStatus.STOPPING)

    def stoppingOrProcessing(worker: ActorRef): ScheduleInstance =
      byStatus(worker, PipegraphStatus.STOPPING, PipegraphStatus.PROCESSING)

    def processing(worker: ActorRef): ScheduleInstance = byStatus(worker, PipegraphStatus.PROCESSING)

    def processing(instanceOf: String): ScheduleInstance = byStatus(instanceOf, PipegraphStatus.PROCESSING)

    def processing(address: Address): Seq[ScheduleInstance] =
      scheduleInstances
        .filter(_.pipegraphInstance.status == PipegraphStatus.PROCESSING)
        .filter(_.worker.path.address == address)

    private def byStatus(instanceOf: String, pipegraphStatus: PipegraphStatus) =
      scheduleInstances
        .filter(_.pipegraphInstance.status == pipegraphStatus)
        .find(_.instanceOf == instanceOf)
        .head

    private def byStatus(worker: ActorRef, pipegraphStatus: PipegraphStatus*) =
      scheduleInstances
        .filter(instance => pipegraphStatus.contains(instance.pipegraphInstance.status))
        .find(_.worker == worker)
        .head

    def isPending(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.PENDING)

    private def isInStatus(instanceOf: String, allowed: PipegraphStatus*) =
      allowed.exists(a => byStatus.get(a).exists(_.contains(instanceOf)))

    def isProcessing(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.PROCESSING)

    def canGoToPending(instanceOf: String): Boolean =
      doesNotKnow(instanceOf)

    def doesNotKnow(instanceOf: String): Boolean = !knows(instanceOf)

    def knows(instanceOf: String): Boolean = known.contains(instanceOf)

    def canGoToStopping(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.PROCESSING)

    def canGoToStopped(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.STOPPING, PipegraphStatus.PENDING)

    def canGoToProcessing(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.STOPPING, PipegraphStatus.PENDING)

    def canGoToSuccessful(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.PROCESSING)

    def canGoToFailed(instanceOf: String): Boolean =
      isInStatus(instanceOf, PipegraphStatus.PROCESSING)
  }

}
