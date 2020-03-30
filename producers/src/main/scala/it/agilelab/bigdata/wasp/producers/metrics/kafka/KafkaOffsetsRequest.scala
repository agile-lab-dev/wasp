package it.agilelab.bigdata.wasp.producers.metrics.kafka

import akka.actor.ActorRef

case class KafkaOffsetsRequest(replyTo: ActorRef,
                               topic: String,
                               ts: Long)
