package it.agilelab.bigdata.wasp.core.kafka

import it.agilelab.bigdata.wasp.core.WaspMessage
import it.agilelab.bigdata.wasp.models.configuration.KafkaConfigModel

trait KafkaAdminMessage extends WaspMessage

case class AddTopic(topic: String = KafkaAdminActor.topic, partitions: Int = KafkaAdminActor.partitions, replicas: Int = KafkaAdminActor.replicas) extends KafkaAdminMessage

case class CheckTopic(topic: String = KafkaAdminActor.topic) extends KafkaAdminMessage

case class RemoveTopic(topic: String = KafkaAdminActor.topic) extends KafkaAdminMessage

case class CheckOrCreateTopic(topic: String = KafkaAdminActor.topic, partitions: Int = KafkaAdminActor.partitions, replicas: Int = KafkaAdminActor.replicas) extends KafkaAdminMessage

case class Initialization(kafkaConfigModel: KafkaConfigModel) extends KafkaAdminMessage