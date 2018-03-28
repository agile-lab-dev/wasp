package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import akka.actor.{FSM, Props}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.StructuredStreamingReader
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps.WriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.State._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{Protocol => MyProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.{Protocol => PipegraphProtocol}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

class StructuredStreamingETLActor private(override val reader: StructuredStreamingReader,
                                          override val plugins: Map[String, WaspConsumersSparkPlugin],
                                          override val sparkSession: SparkSession,
                                          override val mlModelBl: MlModelBL,
                                          override val topicsBl: TopicBL,
                                          override val writerFactory: WriterFactory,
                                          val pipegraph: PipegraphModel
                                         )
  extends FSM[State, Data]
    with ActivationSteps
    with MaterializationSteps
    with MonitoringStep
    with StoppingStep {

  startWith(WaitingToBeActivated, IdleData)

  when(WaitingToBeActivated) {
    case Event(MyProtocol.ActivateETL(etl), IdleData) =>

      activate(etl) match {
        case Success(dataFrame) => goto(WaitingToBeMaterialized) using ActivatedData(dataFrame) replying Protocol.ETLActivated(etl)
        case Failure(reason) =>
          sender() ! MyProtocol.ETLNotActivated(etl, reason)
          stop(FSM.Failure(reason))
      }

  }

  when(WaitingToBeMaterialized) {
    case Event(MyProtocol.MaterializeETL(etl), ActivatedData(dataFrame)) =>
      materialize(etl, pipegraph, dataFrame) match {
        case Success(streamingQuery) => goto(WaitingToBeMonitored) using MaterializedData(streamingQuery) replying Protocol
          .ETLMaterialized(etl)
        case Failure(reason) => goto(WaitingToBeMaterialized) using ActivatedData(dataFrame) replying MyProtocol
          .ETLNotMaterialized(etl, reason)
      }

    case Event(MyProtocol.StopETL(etl), ActivatedData(_)) =>
      sender() ! MyProtocol.ETLStopped(etl)
      stop()
  }

  when(WaitingToBeMonitored) {
    case Event(MyProtocol.CheckETL(etl), MaterializedData(query)) =>
      monitor(query) match {
        case Success(monitoringInfo) => monitoringInfo match {
          case MonitorOutcome(_, _, _, Some(failure)) =>
            goto(WaitingToBeMonitored) using MaterializedData(query) replying MyProtocol.ETLCheckFailed(etl, failure)
          case MonitorOutcome(_, _, _, None) =>
            goto(WaitingToBeMonitored) using MaterializedData(query) replying MyProtocol.ETLCheckSucceeded(etl)
        }

        case Failure(reason) =>
          goto(WaitingToBeMonitored) using MaterializedData(query) replying MyProtocol.ETLCheckFailed(etl, reason)
      }

    case Event(MyProtocol.StopETL(etl), MaterializedData(query)) =>
      stop(query) match {
        case Success(_) =>
          sender() ! MyProtocol.ETLStopped(etl)
          stop()
        case Failure(reason) =>
          stop(FSM.Failure(reason))
      }
  }

  initialize()

}


object StructuredStreamingETLActor {


  def props(reader: StructuredStreamingReader,
            plugins: Map[String, WaspConsumersSparkPlugin],
            sparkSession: SparkSession,
            mlModelBl: MlModelBL,
            topicsBl: TopicBL,
            writerFactory: WriterFactory,
            pipegraph: PipegraphModel) = Props(new StructuredStreamingETLActor(reader,
                                                                               plugins,
                                                                               sparkSession,
                                                                               mlModelBl,
                                                                               topicsBl,
                                                                               writerFactory,
                                                                               pipegraph))

}


