package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import akka.actor.{ActorRef, ActorRefFactory, FSM}
import State._
import Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.{Protocol => MasterProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{Protocol => ChildrenProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => MyProtocol}

import PipegraphGuardian._
import it.agilelab.bigdata.wasp.core.models.StructuredStreamingETLModel

import scala.concurrent.duration.FiniteDuration

class PipegraphGuardian(private val master: ActorRef,
                        private val childFactory: ChildFactory,
                        private val retryDuration: FiniteDuration,
                        private val monitoringInterval: FiniteDuration,
                        private val componentFailedStrategy: ComponentFailedStrategy) extends FSM[State, Data] {

  startWith(WaitingForWork, Empty)

  when(WaitingForWork) {
    case Event(MasterProtocol.WorkAvailable, Empty) =>
      log.debug("Work is available")
      goto(RequestingWork)
  }

  when(RequestingWork) {
    case Event(MasterProtocol.WorkNotGiven(reason), Empty) =>
      log.error(reason, "Work was not given by master [{}]", sender())
      goto(RequestingWork)
    case Event(MasterProtocol.WorkGiven(pipegraph, instance), Empty) =>
      log.debug("Received work, [{}] [{}]", pipegraph.name, instance.name)
      goto(Activating) using ActivatingData(pipegraph, instance,Set.empty,pipegraph.structuredStreamingComponents.toSet)
  }

  when(Activating) {

    case Event(MyProtocol.ActivateETL(etl), data: ActivatingData) =>

      log.debug("Activating etl [{}]", etl.name)

      val newAssociation = WorkerToEtlAssociation(context.watch(childFactory(context)), etl)

      newAssociation.worker ! ChildrenProtocol.ActivateETL(etl)

      val newData = data.copy(toBeActivated = data.toBeActivated - etl,
                              activating = data.activating + newAssociation)

      goto(Activating) using newData

    case Event(ChildrenProtocol.ETLActivated(etl), data: ActivatingData) =>

      log.debug("Activated etl [{}]", etl.name)

      val association = WorkerToEtlAssociation(sender(),etl)

      val newData = data.copy(activating = data.activating - association,
                              active = data.active + association)

      goto(Activating) using newData


    case Event(ChildrenProtocol.ETLNotActivated(etl,reason), data:ActivatingData) =>
      log.error(reason, "Could not activate etl [{}]", etl.name)

      val association = WorkerToEtlAssociation(sender(),etl)

      componentFailedStrategy(etl) match {
        case DontCare =>
          log.debug("[{}] DontCare", etl.name)
          goto(Activating) using data.copy(activating = data.activating - association)
        case Retry =>
          log.debug("[{}] Retry", etl.name)
          goto(Activating) using data.copy(activating = data.activating - association,
                                           toBeRetried = data.toBeActivated + etl )
        case StopAll =>
          log.debug("[{}] StopAll", etl.name)
          goto(Activating) using  data.copy(activating = data.activating - association,
                                            shouldStopAll = true)
      }

    case Event(MyProtocol.ActivationFinished, data:ActivatingData) if !data.shouldStopAll =>
      log.debug("Activation finished")
      goto(Activated) using data.createActivatedData()

    case Event(MyProtocol.ActivationFinished, data:ActivatingData) if data.shouldStopAll =>
      log.debug("Activation finished, outcome is stop all")
      goto(Activated) using data.createActivatedData()

  }


  when(Activated) {
    case Event(MyProtocol.CancelWork, data:ActivatedData) =>
      goto(Stopping) using data.createStoppingData()
    case Event(MyProtocol.MaterializePipegraph, data:ActivatedData) =>
      goto(Materializing) using data.createMaterializingData()

  }


  when(Materializing) {
    case Event(MyProtocol.MaterializeETL(worker, etl), data: MaterializingData) =>
      log.debug("Materializing etl [{}] on worker [{}]", etl.name, worker)
      worker ! ChildrenProtocol.MaterializeETL(etl)

      val association = WorkerToEtlAssociation(worker, etl)


      goto(Materializing) using data.copy(toBeMaterialized = data.toBeMaterialized - association,
                                          materializing = data.materializing + association)

    case Event(ChildrenProtocol.ETLMaterialized(etl), data:MaterializingData) =>
      log.debug("Materialized etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      goto(Materializing) using data.copy(materializing = data.materializing - association,
                                          materialized = data.materialized + association)

    case Event(ChildrenProtocol.ETLNotMaterialized(etl), data:MaterializingData) =>
      log.debug("Could not materialize etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      componentFailedStrategy(etl) match {
        case DontCare =>
          log.debug("[{}] DontCare", etl.name)
          goto(Materializing) using data.copy(materializing = data.materializing - association)
        case Retry =>
          log.debug("[{}] Retry", etl.name)
          goto(Materializing) using data.copy(materializing = data.materializing - association,
                                              toBeRetried = data.toBeRetried + association)
        case StopAll =>
          log.debug("[{}] StopAll", etl.name)
          goto(Materializing) using  data.copy(materializing = data.materializing - association,
                                               shouldStopAll = true)
      }

    case Event(MyProtocol.MaterializationFinished, data:MaterializingData) if !data.shouldStopAll =>
      log.debug("Materialization finished")
      goto(Materialized) using data.createMaterializedData()
    case Event(MyProtocol.MaterializationFinished, data:MaterializingData) if data.shouldStopAll =>
      log.debug("Activation finished, outcome is stop all")
      goto(Materialized) using data.createMaterializedData()

  }

  when(Materialized) {
    case Event(MyProtocol.CancelWork, data:MaterializedData) =>
      goto(Stopping) using data.createStoppingData()
    case Event(MyProtocol.MonitorPipegraph, data:MaterializedData) =>
      goto(Monitoring) using data.createMonitoringData()
  }


  when(Monitoring) {
    case Event(MyProtocol.MonitorETL(worker, etl), data: MonitoringData) =>
      log.debug("Monitoring etl [{}] on worker [{}]", etl.name, worker)
      worker ! ChildrenProtocol.CheckETL(etl)

      val association = WorkerToEtlAssociation(worker, etl)

      goto(Monitoring) using data.copy(toBeMonitored = data.toBeMonitored - association,
                                          monitoring = data.monitoring + association)

    case Event(ChildrenProtocol.ETLCheckSucceeded(etl), data:MonitoringData) =>
      log.debug("Monitoring Succeeded on etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      goto(Monitoring) using data.copy(monitoring = data.monitoring - association,
                                       monitored = data.monitored + association)

    case Event(ChildrenProtocol.ETLCheckFailed(etl, reason), data:MonitoringData) =>
      log.error(reason, "Monitoring Failed on etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      componentFailedStrategy(etl) match {
        case DontCare =>
          log.debug("[{}] DontCare", etl.name)
          goto(Monitoring) using data.copy(monitoring = data.monitoring - association)
        case Retry =>
          log.debug("[{}] Retry", etl.name)
          goto(Monitoring) using data.copy(monitoring = data.monitoring - association,
                                           toBeRetried = data.toBeMonitored + association)
        case StopAll =>
          log.debug("[{}] StopAll", etl.name)
          goto(Monitoring) using  data.copy(monitoring = data.monitoring - association,
                                            shouldStopAll = true)
      }

    case Event(MyProtocol.MonitoringFinished, data:MonitoringData) if !data.shouldStopAll =>
      log.debug("Monitoring round finished")
      goto(Materialized) using data.createMonitoredData()
    case Event(MyProtocol.MonitoringFinished, data:MonitoringData) if data.shouldStopAll =>
      log.debug("Monitoring round finished, outcome is stop all")
      goto(Materialized) using data.createMonitoredData()

  }

  when(Monitored) {
    case Event(MyProtocol.CancelWork, data:MonitoredData) =>
      goto(Stopping) using data.createStoppingData()
    case Event(MyProtocol.MonitorPipegraph, data:MonitoredData) =>
      goto(Monitoring) using data.createMonitoringData()
  }

  when(Stopping) {
    case Event(MyProtocol.StopETL(worker, etl), data: StoppingData) =>
      log.debug("Stopping etl [{}] on worker [{}]", etl.name, worker)

      worker ! ChildrenProtocol.StopETL(etl)

      val association = WorkerToEtlAssociation(worker, etl)


      goto(Stopping) using data.copy(toBeStopped= data.toBeStopped - association,
                                          stopping = data.stopping + association)


    case Event(ChildrenProtocol.ETLStopped(etl), data: StoppingData) =>
      log.debug("Stopped etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      goto(Stopping) using data.copy(stopping = data.stopping - association,
                                     stopped = data.stopped + association)

    case Event(MyProtocol.StopFinished, data: StoppingData) =>
      log.debug("Stopping finished")
      goto(Stopped) using data.createStoppedData()

  }

  when(Stopped) {
    case Event(MyProtocol.Shutdown, StoppedData(pipegraph, _)) =>
      MasterProtocol.PipegraphStopped(pipegraph.name)
      stop()
  }


  onTransition {
    case (WaitingForWork, RequestingWork) =>
      log.debug("Requesting work from master [{}]", sender())
      sender() ! MyProtocol.GimmeWork

    case (RequestingWork, RequestingWork) =>
      log.debug("Requesting work from master [{}] retry", sender())
      sender() ! MyProtocol.GimmeWork

    case (RequestingWork, Activating) => nextStateData match {
      case ActivatingData.ToBeActivated(etl) =>
        self ! MyProtocol.ActivateETL(etl)
    }

    case (Activating, Activating) => nextStateData match {
      case ActivatingData.ToBeActivated(etl) =>
        self ! MyProtocol.ActivateETL(etl)
      case ActivatingData.AllActive() | ActivatingData.ShouldStopAll() =>
        self ! MyProtocol.ActivationFinished
      case _ =>

    }


    case (Activating,Activated) => nextStateData match {
      case data: ActivatedData if data.shouldStopAll =>
        self ! MyProtocol.CancelWork
      case data: ActivatedData if !data.shouldStopAll =>
        self ! MyProtocol.MaterializePipegraph

    }

    case (Activated, Materializing) => nextStateData match {
      case MaterializingData.ToBeMaterialized(WorkerToEtlAssociation(worker, data)) =>
        self ! MyProtocol.MaterializeETL(worker, data)
    }

    case (Activated, Stopping) => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker,etl)) =>
        self ! MyProtocol.StopETL(worker, etl)
    }

    case (Monitoring, Stopping)  => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker,etl)) =>
        self ! MyProtocol.StopETL(worker, etl)
    }

    case (Materialized, Stopping) => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker,etl)) =>
        self ! MyProtocol.StopETL(worker, etl)
    }

    case (Stopping, Stopping) => nextStateData match  {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker,etl)) =>
        self ! MyProtocol.StopETL(worker, etl)
      case StoppingData.AllStopped() =>
        self ! MyProtocol.StopFinished
    }

    case (Stopping, Stopped) => nextStateData match {
      case StoppedData(_,_) =>
        self ! MyProtocol.Shutdown
    }

    case (Materializing, Materializing) => nextStateData match {
      case MaterializingData.ToBeMaterialized(WorkerToEtlAssociation(worker, data)) =>
        self ! MyProtocol.MaterializeETL(worker, data)
      case MaterializingData.AllMaterialized() =>
        self ! MyProtocol.MaterializationFinished
    }

    case (Materializing,Materialized) => nextStateData match {
      case data: MaterializedData if data.shouldStopAll =>
        self ! MyProtocol.CancelWork
      case data: MaterializedData if !data.shouldStopAll =>
        self ! MyProtocol.MonitorPipegraph
    }

  }

  initialize()

}

object PipegraphGuardian {


  type ChildFactory = ActorRefFactory => ActorRef

  type ComponentFailedStrategy = StructuredStreamingETLModel => Choice

  sealed trait Choice

  case class RetryEnvelope[O](original: O, sender: ActorRef)

  case object Retry extends Choice

  case object DontCare extends Choice

  case object StopAll extends Choice

  object Timers {
    val gimmeWork = "gimme-work-timer"
  }

}