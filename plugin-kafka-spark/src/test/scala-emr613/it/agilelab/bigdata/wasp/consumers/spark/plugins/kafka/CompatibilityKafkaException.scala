package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

object CompatibilityKafkaException {

  val unresolvedColTopic = "Expected column named `topic` for topic TOPIC-A to be used as topic, but found None: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `topic` cannot be resolved. Did you mean one of the following? [`id`, `name`, `surname`]."
  val unresolvedColKey = "Expected column named `id-WRONG` for topic TOPIC-A to be used as key, but found None: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `id-WRONG` cannot be resolved. Did you mean one of the following? [`id`, `name`, `surname`, `topic`]."

}
