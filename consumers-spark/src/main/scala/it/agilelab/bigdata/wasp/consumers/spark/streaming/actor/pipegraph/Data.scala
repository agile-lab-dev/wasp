package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, StructuredStreamingETLModel}

/**
  * Trait marking classes holding [[PipegraphGuardian]] State Data
  */
sealed trait Data

object Data {

  type Associations = Set[WorkerToEtlAssociation]


  case class WorkerToEtlAssociation(worker: ActorRef, etl: StructuredStreamingETLModel)

  /**
    * Data of the [[State.Activating]]
    *
    * @param pipegraph The pipegraph being activated
    * @param instance The instance of the pipegraph being Activated
    * @param materialized The already materialized Associations
    * @param toBeActivated The etls to be activated
    * @param activating The etl currently activating
    * @param active The active etls
    * @param toBeRetried The etl activations that should be retried
    * @param shouldStopAll Whether all current active etls should be stopped due to a failure in another etl
    * @param reason Maybe a reason of the failure
    */
  case class ActivatingData(pipegraph: PipegraphModel,
                            instance: PipegraphInstanceModel,
                            materialized: Associations = Set.empty,
                            toBeActivated: Set[StructuredStreamingETLModel] = Set.empty,
                            activating: Associations = Set.empty,
                            active: Associations = Set.empty,
                            toBeRetried: Set[StructuredStreamingETLModel] = Set.empty,
                            shouldStopAll: Boolean = false,
                            reason:Option[Throwable] = None) extends Data {

    def createActivatedData() =
      ActivatedData(pipegraph, instance, materialized, active, toBeRetried, shouldStopAll,reason)
  }


  /**
    * Data of the [[State.Activated]]
    *
    * @param pipegraph The pipegraph being activated
    * @param instance The instance of the pipegraph being Activated
    * @param materialized The already materialized Associations
    * @param active The active etls
    * @param toBeRetried The etl activations that should be retried
    * @param shouldStopAll Whether all current active etls should be stopped due to a failure in another etl
    * @param reason Maybe a reason of the failure
    */
  case class ActivatedData(pipegraph: PipegraphModel,
                           instance: PipegraphInstanceModel,
                           materialized: Associations = Set.empty,
                           active: Associations = Set.empty,
                           toBeRetried: Set[StructuredStreamingETLModel] = Set.empty,
                           shouldStopAll: Boolean = false,
                           reason:Option[Throwable]) extends Data {

    def createStoppingData() =
      StoppingData(pipegraph, instance, materialized ++ active, reason = reason)

    def createMaterializingData() =
      MaterializingData(pipegraph, instance, active, Set.empty, materialized)

  }

  /**
    * Data of the [[State.Materializing]]
    *
    * @param pipegraph The pipegraph being materialized
    * @param instance The instance of the pipegraph being Materialized
    * @param toBeMaterialized The etls to be materialized
    * @param materializing The etl currently materializing
    * @param materialized The already materialized Associations
    * @param toBeRetried The etl materializations that should be retried
    * @param shouldStopAll Whether all current active etls should be stopped due to a failure in another etl
    * @param reason Maybe a reason of the failure
    */
  case class MaterializingData(pipegraph: PipegraphModel,
                               instance: PipegraphInstanceModel,
                               toBeMaterialized: Associations = Set.empty,
                               materializing: Associations = Set.empty,
                               materialized: Associations = Set.empty,
                               toBeRetried: Associations = Set.empty,
                               shouldStopAll:Boolean = false,
                               reason: Option[Throwable]=None) extends Data {

    def createMaterializedData(): Data = MaterializedData(pipegraph,
                                                          instance,
                                                          materialized,
                                                          toBeRetried,
                                                          shouldStopAll,
                                                          reason)

  }

  /**
    * Data of the [[State.Materialized]]
    *
    * @param pipegraph The pipegraph materialized
    * @param instance THe instance of the pipegraph materialized
    * @param materialized THe etl materialized
    * @param toBeRetried The etl to be retried
    * @param shouldStopAll Whether all current materialized etls should be stopped due to a failure in another etl
    * @param reason Maybe a reason of the failure
    */
  case class MaterializedData(pipegraph: PipegraphModel,
                              instance: PipegraphInstanceModel,
                              materialized: Associations = Set.empty,
                              toBeRetried: Associations = Set.empty,
                              shouldStopAll: Boolean = false,
                              reason: Option[Throwable] = None) extends Data {

    def createStoppingData() =
      StoppingData(pipegraph, instance, materialized, reason = reason)

    def createMonitoringData() =
      MonitoringData(pipegraph, instance, materialized)

  }


  /**
    * Data of the [[State.Monitoring]]
    *
    * @param pipegraph The pipegraph being monitored
    * @param instance The instance of the pipegraph being monitored
    * @param toBeMonitored The etls to be monitored
    * @param monitoring The etl currently monitoring
    * @param monitored The already monitored Associations
    * @param toBeRetried The etl monitoring that should be retried
    * @param shouldStopAll Whether all current active etls should be stopped due to a failure in another etl
    * @param reason Maybe a reason of the failure
    */
  case class MonitoringData(pipegraph: PipegraphModel,
                            instance: PipegraphInstanceModel,
                            toBeMonitored: Associations = Set.empty,
                            monitoring: Associations = Set.empty,
                            monitored: Associations = Set.empty,
                            toBeRetried:Associations = Set.empty,
                            shouldStopAll:Boolean = false,
                            reason:Option[Throwable] = None) extends Data {

    def createMonitoredData(): Data =
      MonitoredData(pipegraph,instance,monitored,toBeRetried,shouldStopAll,reason)


  }


  /**
    * Data of the [[State.Monitored]]
    *
    * @param pipegraph The pipegraph monitored
    * @param instance THe instance of the pipegraph monitored
    * @param monitored THe etl monitored
    * @param toBeRetried The etl to be retried
    * @param shouldStopAll Whether all current monitored etls should be stopped due to a failure in another etl
    * @param reason Maybe a reason of the failure
    */
  case class MonitoredData(pipegraph: PipegraphModel,
                           instance: PipegraphInstanceModel,
                           monitored: Associations = Set.empty,
                           toBeRetried: Associations=Set.empty,
                           shouldStopAll:Boolean = false,
                           reason: Option[Throwable]) extends Data {
    def createActivatingData(): Data =
      ActivatingData(pipegraph, instance,monitored,toBeRetried.map(_.etl).toSet )


    def createStoppingData(): Data =
      StoppingData(pipegraph, instance, monitored,reason = reason)

    def createMonitoringData(): Data =
      MonitoringData(pipegraph,instance, monitored)

  }


  /**
    * Data of the [[State.Stopping]]
    *
    * @param pipegraph The pipegraph being stopped
    * @param instance The instance of the pipegraph being stopped
    * @param toBeStopped The etls to be stopped
    * @param stopping The etl currently stopping
    * @param stopped The already stopped Associations
    * @param reason Maybe a reason of the failure
    */
  case class StoppingData(pipegraph: PipegraphModel,
                          instance: PipegraphInstanceModel,
                          toBeStopped: Associations = Set.empty,
                          stopping: Associations = Set.empty,
                          stopped: Associations = Set.empty,
                          reason: Option[Throwable]) extends Data {

    def createStoppedData() =
      StoppedData(pipegraph,instance, reason)
  }


  /**
    * Data of the [[State.Stopped]]
    * @param pipegraph The pipegraph stopped
    * @param instance The instance of the pipegraph stopped
    * @param reason Maybe a reson of failure or nothing (signals normal stopping)
    */
  case class StoppedData(pipegraph: PipegraphModel,
                         instance: PipegraphInstanceModel,
                         reason: Option[Throwable] ) extends Data

  case object Empty extends Data


  object StoppingData {

    /**
      * Extractor of association to be stopped
      */
    object ToBeStopped {
      def unapply(arg: StoppingData): Option[WorkerToEtlAssociation] =
        arg.toBeStopped.headOption
    }

    /**
      * Extractor returning true if all stopped
      */
    object AllStopped {
      def unapply(arg: StoppingData): Boolean = arg.toBeStopped.isEmpty && arg.stopping.isEmpty
    }
  }


  object ActivatingData {

    /**
      * Extractor for association to be activated
      */
    object ToBeActivated {
      def unapply(arg: ActivatingData): Option[StructuredStreamingETLModel] =
        arg.toBeActivated.headOption
    }


    /**
      * Extractor returning true if all Active
      */
    object AllActive {

      def unapply(arg: ActivatingData): Boolean = arg.toBeActivated.isEmpty &&
                                                  arg.activating.isEmpty &&
                                                  arg.toBeRetried.isEmpty

    }

    /**
      * Extractor returning true if should retry
      */
    object ShouldRetry {

      def unapply(arg: ActivatingData): Boolean =  arg.toBeActivated.isEmpty &&
                                                   arg.activating.isEmpty &&
                                                   arg.toBeRetried.nonEmpty

    }

    /**
      * Extractor returning true if should stop all
      */
    object ShouldStopAll {
      def unapply(arg: ActivatingData): Boolean = arg.shouldStopAll
    }

    /**
      * Extractor returning true if should go to materializing
      */
    object ShouldMaterialize {
      def unapply(arg: ActivatingData): Boolean = !arg.shouldStopAll
    }

  }


  object MaterializingData {

    /**
      * Extractor for association to be materialized
      */
    object ToBeMaterialized {
      def unapply(arg: MaterializingData): Option[WorkerToEtlAssociation] =
        arg.toBeMaterialized.headOption
    }

    /**
      * Extractor returning true if should retry
      */
    object ShouldRetry {

      def unapply(arg: MaterializingData): Boolean =  arg.toBeMaterialized.isEmpty &&
                                                      arg.materializing.isEmpty &&
                                                      arg.toBeRetried.nonEmpty

    }

    /**
      * Extractor returning true if all materialized
      */
    object AllMaterialized {
      def unapply(arg: MaterializingData):Boolean = arg.toBeMaterialized.isEmpty &&
                                                    arg.materializing.isEmpty &&
                                                    arg.toBeRetried.isEmpty
    }
    /**
      * Extractor returning true if should stop all
      */
    object ShouldStopAll {
      def unapply(arg: MaterializingData): Boolean = arg.shouldStopAll
    }

  }

  object MonitoringData {

    /**
      * Extractor returning true if should retry
      */
    object ShouldRetry {

      def unapply(arg: MonitoringData): Boolean = arg.toBeRetried.nonEmpty

    }

    /**
      * Extractor for association to be monitored
      */
    object ToBeMonitored {
      def unapply(arg: MonitoringData): Option[WorkerToEtlAssociation] = arg.toBeMonitored.headOption
    }

    object AllMonitored {
      def unapply(arg: MonitoringData):Boolean = arg.toBeMonitored.isEmpty &&
                                                 arg.monitoring.isEmpty
    }
    /**
      * Extractor returning true if should stop all
      */
    object ShouldStopAll {
      def unapply(arg: MonitoringData): Boolean = arg.shouldStopAll
    }


  }

  object MonitoredData {

    /**
      * Extractor returning true if should retry
      */
    object ShouldRetry {

      def unapply(arg: MonitoredData): Boolean = arg.toBeRetried.nonEmpty

    }

    /**
      * Extractor returning true if should stop all
      */
    object ShouldStopAll {
      def unapply(arg: MonitoredData): Boolean = arg.shouldStopAll
    }

    /**
      * Extractor returning true if should continue monitoring
      */
    object ShouldMonitorAgain {
      def unapply(arg: MonitoredData): Boolean = arg.toBeRetried.isEmpty && arg.monitored.nonEmpty
    }

    /**
      * Extractor returning true if nothing is left to monitor
      */
    object NothingToMonitor {
      def unapply(arg: MonitoredData): Boolean = arg.toBeRetried.isEmpty && arg.monitored.isEmpty
    }

  }

}