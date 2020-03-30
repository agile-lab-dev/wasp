package it.agilelab.bigdata.wasp.producers.metrics.kafka

case class KafkaOffsets(topic: String, offs: Map[Int, Long], ts: Long)

