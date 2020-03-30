package it.agilelab.bigdata.wasp.producers.metrics.kafka.backlog

case class BacklogInfo(etlName: String, topicName: String, backlogSize: Long, timestamp: Long)
