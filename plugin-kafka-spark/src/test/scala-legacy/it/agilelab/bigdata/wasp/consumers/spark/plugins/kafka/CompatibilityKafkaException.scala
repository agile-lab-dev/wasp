package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

object CompatibilityKafkaException {

  val unresolvedColTopic = """Expected column named `topic` for topic TOPIC-A to be used as topic, but found None: Cannot resolve column name "topic" among (id, name, surname);"""
  val unresolvedColKey = """Expected column named `id-WRONG` for topic TOPIC-A to be used as key, but found None: Cannot resolve column name "id-WRONG" among (id, name, surname, topic);"""

}
