package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.{ActorRef, ActorRefFactory, FSM, Props}
import State._
import Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.{Protocol => MasterProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{StructuredStreamingETLActor, Protocol => ChildrenProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => MyProtocol}
import PipegraphGuardian._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StructuredStreamingReader
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps.WriterFactory
import it.agilelab.bigdata.wasp.core.bl.{MlModelBL, PipegraphBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models.{PipegraphModel, StructuredStreamingETLModel}
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.FiniteDuration

class PipegraphGuardian(private val master: ActorRef,
                        private val childFactory: ChildFactory,
                        private val retryDuration: FiniteDuration,
                        private val monitoringInterval: FiniteDuration,
                        private val componentFailedStrategy: ComponentFailedStrategy) extends FSM[State, Data] {

  startWith(WaitingForWork, Empty)

  when(WaitingForWork) {
    case Event(MasterProtocol.WorkAvailable, Empty) =>
      log.info("Work is available")
      goto(RequestingWork)
  }

  when(RequestingWork) {
    case Event(MasterProtocol.WorkNotGiven(reason), Empty) =>
      log.error(reason, "Work was not given by master [{}]", sender())
      goto(RequestingWork)
    case Event(MasterProtocol.WorkGiven(pipegraph, instance), Empty) =>
      log.info("Received work, [{}] [{}]", pipegraph.name, instance.name)
      goto(Activating) using ActivatingData(pipegraph, instance, Set.empty, pipegraph.structuredStreamingComponents.toSet)
  }

  when(Activating) {
    case Event(MyProtocol.CancelWork, data: ActivatingData) =>
      goto(Activating) using data.copy(shouldStopAll = true)

    case Event(MyProtocol.ActivateETL(etl), data: ActivatingData) =>

      log.info("Activating etl [{}]", etl.name)

      val newAssociation = WorkerToEtlAssociation(childFactory(data.pipegraph, etl.name, context), etl)

      newAssociation.worker ! ChildrenProtocol.ActivateETL(etl)

      val newData = data.copy(toBeActivated = data.toBeActivated - etl,
        activating = data.activating + newAssociation)

      goto(Activating) using newData

    case Event(ChildrenProtocol.ETLActivated(etl), data: ActivatingData) =>

      log.info("Activated etl [{}]", etl.name)

      val association = WorkerToEtlAssociation(sender(), etl)

      val newData = data.copy(activating = data.activating - association,
        active = data.active + association)

      goto(Activating) using newData


    case Event(ChildrenProtocol.ETLNotActivated(etl, reason), data: ActivatingData) =>
      log.error(reason, "Could not activate etl [{}]", etl.name)

      val association = WorkerToEtlAssociation(sender(), etl)

      componentFailedStrategy(etl) match {
        case DontCare =>
          log.info("[{}] DontCare", etl.name)
          goto(Activating) using data.copy(activating = data.activating - association)
        case Retry =>
          log.info("[{}] Retry", etl.name)
          goto(Activating) using data.copy(activating = data.activating - association,
            toBeRetried = data.toBeActivated + etl)
        case StopAll =>
          log.info("[{}] StopAll", etl.name)
          goto(Activating) using data.copy(activating = data.activating - association,
            shouldStopAll = true,
            reason = Some(reason))
      }

    case Event(MyProtocol.PerformRetry, data: ActivatingData) =>
      log.info("Activation round finished, performing retry")
      goto(Activating) using data.copy(toBeActivated = data.toBeRetried,
        toBeRetried = Set.empty)

    case Event(MyProtocol.ActivationFinished, data: ActivatingData) if !data.shouldStopAll =>
      log.info("Activation finished")
      goto(Activated) using data.createActivatedData()

    case Event(MyProtocol.ActivationFinished, data: ActivatingData) if data.shouldStopAll =>
      log.info("Activation finished, outcome is stop all")
      goto(Activated) using data.createActivatedData()


  }


  when(Activated) {
    case Event(MyProtocol.CancelWork, data: ActivatedData) =>
      goto(Stopping) using data.createStoppingData()
    case Event(MyProtocol.MaterializePipegraph, data: ActivatedData) =>
      goto(Materializing) using data.createMaterializingData()

  }


  when(Materializing) {
    case Event(MyProtocol.CancelWork, data: MaterializingData) =>
      goto(Materializing) using data.copy(shouldStopAll = true)

    case Event(MyProtocol.MaterializeETL(worker, etl), data: MaterializingData) =>
      log.info("Materializing etl [{}] on worker [{}]", etl.name, worker)
      worker ! ChildrenProtocol.MaterializeETL(etl)

      val association = WorkerToEtlAssociation(worker, etl)


      goto(Materializing) using data.copy(toBeMaterialized = data.toBeMaterialized - association,
        materializing = data.materializing + association)

    case Event(ChildrenProtocol.ETLMaterialized(etl), data: MaterializingData) =>
      log.info("Materialized etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      goto(Materializing) using data.copy(materializing = data.materializing - association,
        materialized = data.materialized + association)

    case Event(ChildrenProtocol.ETLNotMaterialized(etl, reason), data: MaterializingData) =>
      log.info("Could not materialize etl [{}] on worker [{}] reason: [{}]", etl.name, sender(), reason)

      val association = WorkerToEtlAssociation(sender(), etl)

      componentFailedStrategy(etl) match {
        case DontCare =>
          log.info("[{}] DontCare", etl.name)
          goto(Materializing) using data.copy(materializing = data.materializing - association)
        case Retry =>
          log.info("[{}] Retry", etl.name)
          goto(Materializing) using data.copy(materializing = data.materializing - association,
            toBeRetried = data.toBeRetried + association)
        case StopAll =>
          log.info("[{}] StopAll", etl.name)
          goto(Materializing) using data.copy(materializing = data.materializing - association,
            shouldStopAll = true,
            reason = Some(reason))
      }

    case Event(MyProtocol.PerformRetry, data: MaterializingData) =>
      log.info("Materialization round finished, performing retry")
      goto(Materializing) using data.copy(toBeMaterialized = data.toBeRetried,
        toBeRetried = Set.empty)


    case Event(MyProtocol.MaterializationFinished, data: MaterializingData) if !data.shouldStopAll =>
      log.info("Materialization finished")
      goto(Materialized) using data.createMaterializedData()
    case Event(MyProtocol.MaterializationFinished, data: MaterializingData) if data.shouldStopAll =>
      log.info("Activation finished, outcome is stop all")
      goto(Materialized) using data.createMaterializedData()

  }

  when(Materialized) {
    case Event(MyProtocol.CancelWork, data: MaterializedData) =>
      goto(Stopping) using data.createStoppingData()
    case Event(MyProtocol.MonitorPipegraph, data: MaterializedData) =>
      goto(Monitoring) using data.createMonitoringData()
  }


  when(Monitoring) {
    case Event(MyProtocol.CancelWork, data: MonitoringData) =>
      log.info("Received request to perform shutdown while Monitoring")
      goto(Monitoring) using data.copy(shouldStopAll = true)

    case Event(MyProtocol.MonitorETL(worker, etl), data: MonitoringData) =>
      log.info("Monitoring etl [{}] on worker [{}]", etl.name, worker)
      worker ! ChildrenProtocol.CheckETL(etl)

      val association = WorkerToEtlAssociation(worker, etl)

      goto(Monitoring) using data.copy(toBeMonitored = data.toBeMonitored - association,
        monitoring = data.monitoring + association)

    case Event(ChildrenProtocol.ETLCheckSucceeded(etl), data: MonitoringData) =>
      log.info("Monitoring Succeeded on etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      goto(Monitoring) using data.copy(monitoring = data.monitoring - association,
        monitored = data.monitored + association)

    case Event(ChildrenProtocol.ETLCheckFailed(etl, reason), data: MonitoringData) =>
      log.error(reason, "Monitoring Failed on etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      componentFailedStrategy(etl) match {
        case DontCare =>
          log.info("[{}] DontCare", etl.name)
          goto(Monitoring) using data.copy(monitoring = data.monitoring - association)
        case Retry =>
          log.info("[{}] Retry", etl.name)
          goto(Monitoring) using data.copy(monitoring = data.monitoring - association,
            toBeRetried = data.toBeMonitored + association)
        case StopAll =>
          log.info("[{}] StopAll", etl.name)
          goto(Monitoring) using data.copy(monitoring = data.monitoring - association,
            shouldStopAll = true,
            reason = Some(reason))
      }

    case Event(MyProtocol.MonitoringFinished, data: MonitoringData) if !data.shouldStopAll =>
      log.info("Monitoring round finished")
      goto(Monitored) using data.createMonitoredData()
    case Event(MyProtocol.MonitoringFinished, data: MonitoringData) if data.shouldStopAll =>
      log.info("Monitoring round finished, outcome is stop all")
      goto(Monitored) using data.createMonitoredData()

  }

  when(Monitored) {
    case Event(MyProtocol.CancelWork, data: MonitoredData) =>
      goto(Stopping) using data.createStoppingData()
    case Event(MyProtocol.MonitorPipegraph, data: MonitoredData) =>
      goto(Monitoring) using data.createMonitoringData()
    case Event(MyProtocol.PerformRetry, data: MonitoredData) =>
      goto(Activating) using data.createActivatingData()
  }

  when(Stopping) {
    case Event(MyProtocol.StopETL(worker, etl), data: StoppingData) =>
      log.info("Stopping etl [{}] on worker [{}]", etl.name, worker)

      worker ! ChildrenProtocol.StopETL(etl)

      val association = WorkerToEtlAssociation(worker, etl)


      goto(Stopping) using data.copy(toBeStopped = data.toBeStopped - association,
        stopping = data.stopping + association)


    case Event(ChildrenProtocol.ETLStopped(etl), data: StoppingData) =>
      log.info("Stopped etl [{}] on worker [{}]", etl.name, sender())

      val association = WorkerToEtlAssociation(sender(), etl)

      goto(Stopping) using data.copy(stopping = data.stopping - association,
        stopped = data.stopped + association)

    case Event(MyProtocol.StopFinished, data: StoppingData) =>
      log.info("Stopping finished")
      goto(Stopped) using data.createStoppedData()

  }

  when(Stopped) {
    case Event(MyProtocol.Shutdown, StoppedData(_, _, None)) =>
      master ! MasterProtocol.WorkCompleted
      stop()
    case Event(MyProtocol.Shutdown, StoppedData(_, _, Some(reason))) =>
      master ! MasterProtocol.WorkFailed(reason)
      stop()
  }


  onTransition {
    case (WaitingForWork, RequestingWork) =>
      log.info("Requesting work from master [{}]", sender())
      sender() ! MyProtocol.GimmeWork

    case (RequestingWork, RequestingWork) =>
      log.info("Requesting work from master [{}] retry", sender())
      sender() ! MyProtocol.GimmeWork

    case (RequestingWork, Activating) => nextStateData match {
      case ActivatingData.ToBeActivated(etl) =>
        self ! MyProtocol.ActivateETL(etl)
    }

    case (Activating, Activating) => nextStateData match {
      case ActivatingData.ShouldRetry() =>
        log.info("[Activating->Activating] Scheduling Retry")
        setTimer("retry", MyProtocol.PerformRetry, retryDuration)
      case ActivatingData.ToBeActivated(etl) =>
        log.info("[Activating->Activating] Activating [{}]", etl.name)
        self ! MyProtocol.ActivateETL(etl)
      case ActivatingData.ShouldStopAll() =>
        log.info("[Activating->Activating] StopAll")
        self ! MyProtocol.ActivationFinished
      case ActivatingData.AllActive() =>
        log.info("[Activating->Activating] AllActive")
        self ! MyProtocol.ActivationFinished
      case _ => log.info("[Activating->Activating] Empty transition effect")

    }


    case (Activating, Activated) => nextStateData match {
      case data: ActivatedData if data.shouldStopAll =>
        self ! MyProtocol.CancelWork
      case data: ActivatedData if !data.shouldStopAll =>
        self ! MyProtocol.MaterializePipegraph

    }

    case (Activated, Materializing) => nextStateData match {
      case MaterializingData.ToBeMaterialized(WorkerToEtlAssociation(worker, data)) =>
        self ! MyProtocol.MaterializeETL(worker, data)
      case _ => log.info("[Activating->Materializing] Empty transition effect")
    }

    case (Activated, Stopping) => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker, etl)) =>
        log.info("[Activated->Stopping] Stopping [{}-{}]", worker, etl.name)
        self ! MyProtocol.StopETL(worker, etl)
      case StoppingData.AllStopped() =>
        log.info("[Activated->Stopping] All Stopped")
        self ! MyProtocol.StopFinished
      case _ => log.info("[Activated->Stopping] Empty transition effect")

    }

    case (Monitoring, Stopping) => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker, etl)) =>
        self ! MyProtocol.StopETL(worker, etl)
    }

    case (Materialized, Stopping) => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker, etl)) =>
        log.info("[Materialized->Stopping] Stopping [{}->{}]", worker, etl)
        self ! MyProtocol.StopETL(worker, etl)
      case StoppingData.AllStopped() =>
        log.info("[Materialized->Stopping] All stopped")
        self ! MyProtocol.StopFinished

    }

    case (Materialized, Monitoring) => nextStateData match {
      case MonitoringData.ToBeMonitored(WorkerToEtlAssociation(worker, etl)) =>
        log.info("[Materialized->Monitoring] Monitoring [{}-{}]", worker, etl.name)
        self ! MyProtocol.MonitorETL(worker, etl)
      case MonitoringData.AllMonitored() =>
        log.info("[Materialized->Monitoring] All monitored")
        self ! MyProtocol.MonitoringFinished
    }

    case (Stopping, Stopping) => nextStateData match {
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker, etl)) =>
        self ! MyProtocol.StopETL(worker, etl)
      case StoppingData.AllStopped() =>
        self ! MyProtocol.StopFinished
      case _ => log.info("[Stopping->Stopping] Empty transition effect")
    }

    case (Stopping, Stopped) => nextStateData match {
      case _ =>
        log.info("[Stopping->Stopped] Shutting down")
        self ! MyProtocol.Shutdown
    }

    case (Materializing, Materializing) => nextStateData match {
      case MaterializingData.ShouldRetry() =>
        log.info("[Materializing->Materializing] Scheduling Retry")
        setTimer("retry", MyProtocol.PerformRetry, retryDuration)
      case MaterializingData.ToBeMaterialized(WorkerToEtlAssociation(worker, data)) =>
        log.info("[Materializing->Materializing] materialize [{}->{}]", worker, data.name)
        self ! MyProtocol.MaterializeETL(worker, data)
      case MaterializingData.AllMaterialized() =>
        log.info("[Materializing->Materializing] AllMaterialized")
        self ! MyProtocol.MaterializationFinished
      case MaterializingData.ShouldStopAll() =>
        log.info("[Materializing->Materializing] StopAll")
        self ! MyProtocol.MaterializationFinished
      case _ => log.info("[Materializing->Materializing] Empty transition effect")
    }

    case (Materializing, Materialized) => nextStateData match {
      case data: MaterializedData if data.shouldStopAll =>
        log.info("[Materializing->Materialized] Scheduling StopAll")
        self ! MyProtocol.CancelWork
      case data: MaterializedData if !data.shouldStopAll =>
        log.info("[Materializing->Materialized] Scheduling Monitoring")
        self ! MyProtocol.MonitorPipegraph
    }


    case (Monitoring, Monitoring) => nextStateData match {
      case MonitoringData.ToBeMonitored(WorkerToEtlAssociation(worker, data)) =>
        log.info("[Monitoring->Monitoring] Monitor [{}->{}]", worker, data.name)
        self ! MyProtocol.MonitorETL(worker, data)
      case MonitoringData.AllMonitored() =>
        log.info("[Monitoring->Monitoring] All Monitored")
        self ! MyProtocol.MonitoringFinished
      case MonitoringData.ShouldStopAll() =>
        log.info("[Monitoring->Monitoring] StopAll")
        self ! MyProtocol.MonitoringFinished
      case _ => log.info("[Monitoring->Monitoring] Empty transition effect")
    }

    case (Monitoring, Monitored) => nextStateData match {
      case MonitoredData.ShouldRetry() =>
        log.info("[Monitoring->Monitored] Scheduling Retry")
        setTimer("retry", MyProtocol.PerformRetry, retryDuration)
      case MonitoredData.ShouldStopAll() =>
        log.info("[Monitoring->Monitored] StopAll")
        self ! MyProtocol.CancelWork
      case MonitoredData.ShouldMonitorAgain() =>
        log.info("[Monitoring->Monitored] Scheduling Monitoring")
        setTimer("monitoring", MyProtocol.MonitorPipegraph, monitoringInterval)

      case MonitoredData.NothingToMonitor() =>
        log.info("[Monitoring->Monitored] Nothing to monitor")
        self ! MyProtocol.CancelWork

    }

    case (Monitored, Stopping) => nextStateData match {
      case StoppingData.AllStopped() =>
        log.info("[Monitored->Stopping] All stopped")
        self ! MyProtocol.StopFinished
      case StoppingData.ToBeStopped(WorkerToEtlAssociation(worker, etl)) =>
        log.info("[Monitored->Stopping] Scheduling Stop of [{}->{etl}]", worker, etl)
        self ! MyProtocol.StopETL(worker, etl)

    }

    case (Monitored, Activating) => nextStateData match {
      case ActivatingData.ToBeActivated(etl) =>
        log.info("[Monitored->Activating] Scheduling activation")
        self ! MyProtocol.ActivateETL(etl)


    }

    case (Monitored, Monitoring) => nextStateData match {
      case MonitoringData.ToBeMonitored(WorkerToEtlAssociation(worker, etl)) =>
        log.info("[Monitored->Monitoring] Monitoring [{}-{}]", worker, etl.name)
        self ! MyProtocol.MonitorETL(worker, etl)
      case MonitoringData.AllMonitored() =>
        log.info("[Monitored->Monitoring] All monitored")
        self ! MyProtocol.MonitoringFinished
    }

  }

  initialize()

}

object PipegraphGuardian {

  /**
    * A child factory is a function of (pipegraph,actorName, context | Actorsystem)
    */
  type ChildFactory = (PipegraphModel,String, ActorRefFactory) => ActorRef
  type ComponentFailedStrategy = StructuredStreamingETLModel => Choice

  def defaultChildFactory(reader: StructuredStreamingReader,
                          plugins: Map[String, WaspConsumersSparkPlugin],
                          sparkSession: SparkSession,
                          mlModelBl: MlModelBL,
                          topicsBl: TopicBL,
                          writerFactory: WriterFactory): ChildFactory = { (pipegraph,name, context) =>

    //actor names should be urlsafe
    val saneName = URLEncoder.encode(name.replaceAll(" ", "-"), StandardCharsets.UTF_8.name())

    context.actorOf(StructuredStreamingETLActor.props(reader, plugins, sparkSession, mlModelBl, topicsBl,
      writerFactory, pipegraph),saneName)

  }

  def props(master: ActorRef,
                    childFactory: ChildFactory,
                    retryDuration: FiniteDuration,
                    monitoringInterval: FiniteDuration,
                    componentFailedStrategy: ComponentFailedStrategy) =
    Props(new PipegraphGuardian(master,
      childFactory,
      retryDuration,
      monitoringInterval,
      componentFailedStrategy))

  sealed trait Choice

  case class RetryEnvelope[O](original: O, sender: ActorRef)

  case object Retry extends Choice

  case object DontCare extends Choice

  case object StopAll extends Choice

  object Timers {
    val gimmeWork = "gimme-work-timer"
  }

}