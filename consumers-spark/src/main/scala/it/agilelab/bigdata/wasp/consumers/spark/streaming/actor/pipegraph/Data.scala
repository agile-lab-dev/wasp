package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.models.{LegacyStreamingETLModel, PipegraphInstanceModel, PipegraphModel, StructuredStreamingETLModel}

sealed trait Data

object Data {


  type Associations = Set[WorkerToEtlAssociation]

  case class WorkerToEtlAssociation(worker: ActorRef, etl: StructuredStreamingETLModel)

  case class ActivatingData(pipegraph: PipegraphModel,
                            instance: PipegraphInstanceModel,
                            materialized: Associations = Set.empty,
                            toBeActivated: Set[StructuredStreamingETLModel] = Set.empty,
                            activating: Associations = Set.empty,
                            active: Associations = Set.empty,
                            toBeRetried: Set[StructuredStreamingETLModel] = Set.empty,
                            shouldStopAll: Boolean = false) extends Data {

    def createActivatedData() =
      ActivatedData(pipegraph, instance, materialized, active, toBeRetried, shouldStopAll)
  }

  case class ActivatedData(pipegraph: PipegraphModel,
                           instance: PipegraphInstanceModel,
                           materialized: Associations = Set.empty,
                           active: Associations = Set.empty,
                           toBeRetried: Set[StructuredStreamingETLModel] = Set.empty,
                           shouldStopAll: Boolean = false) extends Data {

    def createStoppingData() =
      StoppingData(pipegraph, instance, materialized ++ active)

    def createMaterializingData() =
      MaterializingData(pipegraph, instance, active, Set.empty, materialized)

  }

  case class MaterializingData(pipegraph: PipegraphModel,
                               instance: PipegraphInstanceModel,
                               toBeMaterialized: Associations = Set.empty,
                               materializing: Associations = Set.empty,
                               materialized: Associations = Set.empty,
                               toBeRetried: Associations = Set.empty,
                               shouldStopAll:Boolean = false) extends Data {

    def createMaterializedData(): Data = MaterializedData(pipegraph,
                                                          instance,
                                                          materialized,
                                                          toBeRetried,
                                                          shouldStopAll)

  }

  case class MaterializedData(pipegraph: PipegraphModel,
                              instance: PipegraphInstanceModel,
                              materialized: Associations = Set.empty,
                              toBeRetried: Associations = Set.empty,
                              shouldStopAll: Boolean = false) extends Data {

    def createStoppingData() =
      StoppingData(pipegraph, instance, materialized)

    def createMonitoringData() =
      MonitoringData(pipegraph, instance, materialized)

  }


  case class MonitoringData(pipegraph: PipegraphModel,
                            instance: PipegraphInstanceModel,
                            toBeMonitored: Associations = Set.empty,
                            monitoring: Associations = Set.empty,
                            monitored: Associations = Set.empty,
                            toBeRetried:Associations = Set.empty,
                            shouldStopAll:Boolean = false) extends Data {

    def createMonitoredData(): Data =
      MonitoredData(pipegraph,instance,monitored,toBeRetried,shouldStopAll)


  }


  case class MonitoredData(pipegraph: PipegraphModel,
                           instance: PipegraphInstanceModel,
                           monitored: Associations = Set.empty,
                           toBeRetried: Associations=Set.empty,
                           shouldStopAll:Boolean = false) extends Data {
    def createActivatingData(): Data =
      ActivatingData(pipegraph, instance,monitored,toBeRetried.map(_.etl).toSet )


    def createStoppingData(): Data =
      StoppingData(pipegraph, instance, monitored)

    def createMonitoringData(): Data =
      MonitoringData(pipegraph,instance, monitored)

  }


  case class StoppingData(pipegraph: PipegraphModel,
                          instance: PipegraphInstanceModel,
                          toBeStopped: Associations = Set.empty,
                          stopping: Associations = Set.empty,
                          stopped: Associations = Set.empty) extends Data {

    def createStoppedData() =
      StoppedData(pipegraph,instance)
  }


  case class StoppedData(pipegraph: PipegraphModel,
                         instance: PipegraphInstanceModel) extends Data

  case object Empty extends Data


  object StoppingData {
    object ToBeStopped {
      def unapply(arg: StoppingData): Option[WorkerToEtlAssociation] =
        arg.toBeStopped.headOption
    }

    object AllStopped {
      def unapply(arg: StoppingData): Boolean = arg.toBeStopped.isEmpty && arg.stopping.isEmpty
    }
  }


  object ActivatingData {

    object ToBeActivated {
      def unapply(arg: ActivatingData): Option[StructuredStreamingETLModel] =
        arg.toBeActivated.headOption
    }

    object AllActive {

      def unapply(arg: ActivatingData): Boolean = arg.toBeActivated.isEmpty &&
                                                  arg.activating.isEmpty &&
                                                  arg.toBeRetried.isEmpty

    }

    object ShouldRetry {

      def unapply(arg: ActivatingData): Boolean =  arg.toBeActivated.isEmpty &&
                                                   arg.activating.isEmpty &&
                                                   arg.toBeRetried.nonEmpty

    }

    object ShouldStopAll {
      def unapply(arg: ActivatingData): Boolean = arg.shouldStopAll
    }

    object ShouldMaterialize {
      def unapply(arg: ActivatingData): Boolean = !arg.shouldStopAll
    }

  }


  object MaterializingData {

    object ToBeMaterialized {
      def unapply(arg: MaterializingData): Option[WorkerToEtlAssociation] =
        arg.toBeMaterialized.headOption
    }

    object ShouldRetry {

      def unapply(arg: MaterializingData): Boolean =  arg.toBeMaterialized.isEmpty &&
                                                      arg.materializing.isEmpty &&
                                                      arg.toBeRetried.nonEmpty

    }

    object AllMaterialized {
      def unapply(arg: MaterializingData):Boolean = arg.toBeMaterialized.isEmpty &&
                                                    arg.materializing.isEmpty &&
                                                    arg.toBeRetried.isEmpty
    }

    object ShouldStopAll {
      def unapply(arg: ActivatingData): Boolean = arg.shouldStopAll
    }

  }

  object MonitoringData {


    object ShouldRetry {

      def unapply(arg: MonitoringData): Boolean = arg.toBeRetried.nonEmpty

    }

    object ToBeMonitored {
      def unapply(arg: MonitoringData): Option[WorkerToEtlAssociation] = arg.toBeMonitored.headOption
    }

    object AllMonitored {
      def unapply(arg: MonitoringData):Boolean = arg.toBeMonitored.isEmpty &&
                                                 arg.monitoring.isEmpty
    }

    object ShouldStopAll {
      def unapply(arg: MonitoringData): Boolean = arg.shouldStopAll
    }


  }

  object MonitoredData {


    object ShouldRetry {

      def unapply(arg: MonitoredData): Boolean = arg.toBeRetried.nonEmpty

    }


    object ShouldStopAll {
      def unapply(arg: MonitoredData): Boolean = arg.shouldStopAll
    }

    object ShouldMonitorAgain {
      def unapply(arg: MonitoredData): Boolean = arg.toBeRetried.isEmpty && arg.monitored.nonEmpty
    }

    object NothingToMonitor {
      def unapply(arg: MonitoredData): Boolean = arg.toBeRetried.isEmpty && arg.monitored.isEmpty
    }

  }

}