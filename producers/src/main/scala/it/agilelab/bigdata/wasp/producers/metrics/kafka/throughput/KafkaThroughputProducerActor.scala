package it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput

import akka.actor.{ActorRef, Cancellable}
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.data.{RingBuffer, RingBufferQueue}
import it.agilelab.bigdata.wasp.producers.metrics.kafka.{KafkaOffsets, KafkaOffsetsRequest}
import it.agilelab.bigdata.wasp.producers.{ProducerActor, StartMainTask, StopMainTask}

import scala.concurrent.duration._

abstract class KafkaThroughputProducerActor[A](kafka_router: ActorRef,
                                               kafkaOffsetChecker: ActorRef,
                                               topic: Option[TopicModel],
                                               topicToCheck: String,
                                               windowSize: Long,
                                               sendMessageEveryXsamples: Int,
                                               triggerIntervalMs: Long) extends ProducerActor[A](kafka_router, topic) {
  //Perfectly fine to have mutable variables because each actor is a single thread
  private var ringBuffer: RingBuffer[Long] = _
  private var currentNumberOfMessages: Option[Long] = _
  private var remainingSamplesBeforeMessage: Int = _
  private var cancellable: Cancellable = new Cancellable {
    override def cancel(): Boolean = true

    override def isCancelled: Boolean = true
  }

  override def preStart(): Unit = {
    ringBuffer = RingBufferQueue.empty(Math.max((windowSize / triggerIntervalMs).toInt, 1))
    currentNumberOfMessages = None
    remainingSamplesBeforeMessage = sendMessageEveryXsamples
  }

  override def receive: Receive = {
    case StopMainTask =>
      cancellable.cancel()
      stopMainTask()
    case StartMainTask =>
      logger.info(s"Requesting offsets: $topicToCheck")
      kafkaOffsetChecker ! KafkaOffsetsRequest(self, topicToCheck, System.currentTimeMillis())
    case o: KafkaOffsets =>
      logger.debug(s"Received offsets: $o")
      val numNewMessages = o.offs.values.sum
      ringBuffer = ringBuffer.push(numNewMessages - currentNumberOfMessages.getOrElse(numNewMessages))
      currentNumberOfMessages = Some(numNewMessages)
      remainingSamplesBeforeMessage -= 1
      logger.debug(s"Sum of current offsets: ${numNewMessages.toString}")
      prepareAndSendMessage()
      cancellable = context.system.scheduler
        .scheduleOnce(triggerIntervalMs.millis)(
          kafkaOffsetChecker ! KafkaOffsetsRequest(self, topicToCheck, System.currentTimeMillis())
        )(context.dispatcher)
  }

  override def mainTask(): Unit = {
    logger.error("No one should call this")
  }

  protected def toFinalMessage(messageSumInWindow: Long, timestamp: Long): A

  private def prepareAndSendMessage(): Unit = {
    //Devo mandare il messaggio
    if (remainingSamplesBeforeMessage == 0) {
      val timestamp = System.currentTimeMillis()

      // Se la finestra non è ancora piena, non mando il risultato
      // Richiedo una finestra temporale con tutti i dati per poter fornire la metrica
      val sumOfMessagesInWindow: Long = if (ringBuffer.isFull) ringBuffer.sumElements else 0L
      logger.debug(s"ringBuffer: $ringBuffer")

      val outputMessage = toFinalMessage(sumOfMessagesInWindow, timestamp)
      sendMessage(outputMessage)
      remainingSamplesBeforeMessage = sendMessageEveryXsamples
      logger.debug(s"$ringBuffer  - Total messages in window: $sumOfMessagesInWindow")
      logger.debug(s"Generated message for the KafkaOffsetChecker with payload: ${outputMessage.toString}")
    } else {
      logger.debug(s"NoOp since remainingSamplesBeforeMessage was $remainingSamplesBeforeMessage")
    }
  }
}
