package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import java.util.UUID
import akka.actor.{ActorRef, ActorRefFactory, FSM, LoggingFSM, Props}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.ActivationSteps.{
  StaticReaderFactory,
  StreamingReaderFactory
}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.Data._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps.WriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.State._
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.StructuredStreamingETLActor.TelemetryActorFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.{Protocol => MyProtocol}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.telemetry.TelemetryActor
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.models.PipegraphModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

import java.time.{Duration, Instant}
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

  val triggerIntervalMultiplier: Double =
    ConfigManager.conf.getDouble("spark-streaming.watchdog.trigger-interval-multiplier")
  val watchdogEnabled: Boolean = ConfigManager.conf.getBoolean("spark-streaming.watchdog.enabled")

  startWith(WaitingToBeActivated, IdleData)

  when(WaitingToBeActivated) { case Event(MyProtocol.ActivateETL(etl), IdleData) =>
    activate(etl, pipegraph) match {
      case Success(dataFrame) =>
        goto(WaitingToBeMaterialized) using ActivatedData(dataFrame) replying Protocol.ETLActivated(etl)
      case Failure(reason) =>
        sender() ! MyProtocol.ETLNotActivated(etl, reason)
        stop(FSM.Failure(reason))
    }

  }

  when(WaitingToBeMaterialized) {
    case Event(MyProtocol.MaterializeETL(etl), ActivatedData(dataFrame)) =>
      materialize(etl, pipegraph, dataFrame) match {
        case Success((streamingQuery, triggerInterval)) =>
          goto(WaitingToBeMonitored) using MaterializedData(
            streamingQuery,
            Instant.now(),
            triggerInterval,
            false
          ) replying Protocol
            .ETLMaterialized(etl)
        case Failure(reason) => {
          goto(WaitingToBeMaterialized) using ActivatedData(dataFrame) replying MyProtocol
            .ETLNotMaterialized(etl, reason)
        }
      }

    case Event(MyProtocol.StopETL(etl), ActivatedData(_)) =>
      sender() ! MyProtocol.ETLStopped(etl)
      stop()
  }

  when(WaitingToBeMonitored) {
    case Event(MyProtocol.CheckETL(etl), MaterializedData(query, materiazationTimestamp, triggerInterval, killed)) =>
      monitor(query) match {
        case Success(monitoringInfo) =>
          monitoringInfo match {
            case MonitorOutcome(_, _, _, Some(failure)) =>
              sender() ! MyProtocol.ETLCheckFailed(etl, failure)
              stop(FSM.Failure(failure))
            case MonitorOutcome(false, _, _, None) =>
              val exception = new Exception(
                if (killed)
                  s"Query has been killed for exceeding trigger interval goal of" +
                    s" triggerInterval * multiplier = [$triggerInterval * $triggerIntervalMultiplier = ${triggerInterval * triggerIntervalMultiplier}] ms"
                else "Query is stopped"
              )
              sender() ! MyProtocol.ETLCheckFailed(etl, exception)
              stop(FSM.Failure(exception))
            case outcome @ MonitorOutcome(_, _, _, None) =>
              telemetryActor ! outcome

              val isNowKilled = if (watchdogEnabled && triggerInterval > 0) {
                checkIfKilled(query, materiazationTimestamp, triggerInterval, outcome)
              } else {
                false
              }

              goto(WaitingToBeMonitored) using MaterializedData(
                query,
                materiazationTimestamp,
                triggerInterval,
                isNowKilled
              ) replying MyProtocol.ETLCheckSucceeded(etl)
          }

        case Failure(reason) =>
          sender() ! MyProtocol.ETLCheckFailed(etl, reason)
          stop(FSM.Failure(reason))
      }

    case Event(MyProtocol.StopETL(etl), MaterializedData(query, _, _, _)) =>
      stop(query) match {
        case Success(_) =>
          sender() ! MyProtocol.ETLStopped(etl)
          stop()
        case Failure(reason) =>
          stop(FSM.Failure(reason))
      }
  }

  private def checkIfKilled(
      query: StreamingQuery,
      materiazationTimestamp: Instant,
      triggerInterval: Long,
      outcome: MonitorOutcome
  ): Boolean = {

    val lastKnownTimestamp = outcome.progress.map(_.timestamp).map(Instant.parse).getOrElse(materiazationTimestamp)
    val now                = Instant.now()
    val stuckFor           = Duration.between(lastKnownTimestamp, now).toMillis

    if (stuckFor > triggerInterval * triggerIntervalMultiplier) {
      query.stop()
      true
    } else {
      false
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

  def defaultTelemetryActorFactory(): TelemetryActorFactory = { (suppliedName, context) =>
    val name = s"$suppliedName-${UUID.randomUUID()}"

    context.actorOf(TelemetryActor.props(), name)

  }

}
