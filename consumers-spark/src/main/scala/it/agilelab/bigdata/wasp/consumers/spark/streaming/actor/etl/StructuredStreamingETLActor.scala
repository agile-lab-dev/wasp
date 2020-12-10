package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import java.util.UUID

import akka.actor.{ActorRef, ActorRefFactory, FSM, LoggingFSM, Props}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.ActivationSteps.{StaticReaderFactory, StreamingReaderFactory}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps.WriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.State._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.StructuredStreamingETLActor.TelemetryActorFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{Protocol => MyProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.telemetry.TelemetryActor
import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.PipegraphModel
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

class StructuredStreamingETLActor private (
    override val sparkSession: SparkSession,
    override val mlModelBl: MlModelBL,
    override val topicsBl: TopicBL,
    override val freeCodeBL: FreeCodeBL,
    override val processGroupBL: ProcessGroupBL,
    override val streamingReaderFactory: StreamingReaderFactory,
    override val staticReaderFactory: StaticReaderFactory,
    override val writerFactory: WriterFactory,
    val pipegraph: PipegraphModel,
    val telemetryActorFactory: TelemetryActorFactory
) extends FSM[State, Data]
    with LoggingFSM[State, Data]
    with ActivationSteps
    with MaterializationSteps
    with MonitoringStep
    with StoppingStep {


  val telemetryActor = telemetryActorFactory("telemetry", context)

  startWith(WaitingToBeActivated, IdleData)

  when(WaitingToBeActivated) {
    case Event(MyProtocol.ActivateETL(etl), IdleData) =>

      activate(etl, pipegraph) match {
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
            sender() ! MyProtocol.ETLCheckFailed(etl, failure)
            stop(FSM.Failure(failure))
          case outcome @ MonitorOutcome(_, _, _, None) =>
            telemetryActor ! outcome
            goto(WaitingToBeMonitored) using MaterializedData(query) replying MyProtocol.ETLCheckSucceeded(etl)
        }

        case Failure(reason) =>
          sender() ! MyProtocol.ETLCheckFailed(etl, reason)
          stop(FSM.Failure(reason))
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

  type TelemetryActorFactory = (String, ActorRefFactory) => ActorRef

  def props(
      sparkSession: SparkSession,
      mlModelBl: MlModelBL,
      topicsBl: TopicBL,
      freeCodeBL: FreeCodeBL,
      processGroupBL: ProcessGroupBL,
      streamingReaderFactory: StreamingReaderFactory,
      staticReaderFactory: StaticReaderFactory,
      writerFactory: WriterFactory,
      pipegraph: PipegraphModel,
      telemetryActorFactory: TelemetryActorFactory
  ) =
    Props(
      new StructuredStreamingETLActor(
        sparkSession,
        mlModelBl,
        topicsBl,
        freeCodeBL,
        processGroupBL,
        streamingReaderFactory,
        staticReaderFactory,
        writerFactory,
        pipegraph,
        telemetryActorFactory
      )
    )

  def defaultTelemetryActorFactory() : TelemetryActorFactory = { (suppliedName, context) =>

    val name = s"$suppliedName-${UUID.randomUUID()}"

    context.actorOf(TelemetryActor.props(), name)

  }

}


