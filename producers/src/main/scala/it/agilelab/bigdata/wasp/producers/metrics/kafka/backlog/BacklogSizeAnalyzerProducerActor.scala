package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.messages.{TelemetryMessageSource, TelemetryMessageSourcesSummary}
import it.agilelab.bigdata.wasp.models.TopicModel
import it.agilelab.bigdata.wasp.producers.ProducerActor
import it.agilelab.bigdata.wasp.producers.metrics.kafka.{KafkaOffsets, KafkaOffsetsRequest}

abstract class BacklogSizeAnalyzerProducerActor[A](kafka_router: ActorRef,
                                                   kafkaOffsetChecker: ActorRef,
                                                   topic: Option[TopicModel],
                                                   topicToCheck: String,
                                                   etlName: String) extends ProducerActor[A](kafka_router, topic) {

  override def preStart(): Unit = {
    context.become(waitingForMessage())
  }

  override def mainTask(): Unit = {}

  private def waitingForMessage(): Receive = {
    case data: TelemetryMessageSourcesSummary =>
      logger.debug(s"Received message with data: $data")
      // This will probably change when multi streaming sources are enabled
      // It is safe to take directly the actor ref, because the actors are not spawned if their guardian is
      // not connected to the KafkaOffsetCheckerProducerGuardian
      kafkaOffsetChecker ! KafkaOffsetsRequest(self, topicToCheck, System.currentTimeMillis())
      context become waitingForOffsets(data)

    case _: KafkaOffsets => logger.warn("Received message with KafkaOffsets too late. Discarding message.")

    case unkMessage =>
      logger.warn(s"Unexpected message received ${unkMessage.toString} from $sender()")
  }

  private def waitingForOffsets(data: TelemetryMessageSourcesSummary): Receive = {
    case KafkaOffsets(_, offsets, _) =>
      calculateBacklogSize(offsets, data) match {
        case Left(error) =>
          logger.warn(error)
          context become waitingForMessage()
        case Right(difference) =>
          val infoToPush = BacklogInfo(etlName, topicToCheck, difference, System.currentTimeMillis())
          prepareToSendMessage(infoToPush)
          logger.debug(s"Sent message for: $infoToPush")
          context become waitingForMessage()
      }

    case newData: TelemetryMessageSourcesSummary =>
      logger.debug("Too much time waiting for the kafka response, going to evaluate new Spark data")
      context become waitingForOffsets(newData)

    case unkMessage =>
      logger.warn(s"Unexpected message received ${unkMessage.toString} from $sender()")
  }

  private def calculateBacklogSize(offsetsOnKafka: Map[Int, Long], data: TelemetryMessageSourcesSummary): Either[String, Long] = {

    val streamingSources: Seq[TelemetryMessageSource] = data.streamingQueriesProgress

    logger.debug(s"Evaluating streamingSources:\n${streamingSources.mkString("\n\t")}")
    streamingSources.flatMap { source =>
      source.endOffset.get(topicToCheck).map { map =>
        map.map { case (k, v) =>
          k.toInt -> v
        }
      }
    }.headOption match {
      case Some(endOffset) =>
        logger.debug(s"Current end offsets of Spark Streaming query:\n" +
          endOffset.map { case (k, v) => k + "->" + v }.mkString("\n\t"))
        Right(offsetsOnKafka.map { case (i, o) => o - endOffset(i) }.sum)
      case None =>
        Left(s"Streaming sources did not contain info about topic ${topicToCheck}")
    }
  }

  private def prepareToSendMessage(info: BacklogInfo): Unit = {
    sendMessage(toFinalMessage(info))
  }

  def toFinalMessage(i: BacklogInfo): A

}

