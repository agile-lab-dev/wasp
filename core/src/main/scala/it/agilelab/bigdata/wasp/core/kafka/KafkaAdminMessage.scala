package it.agilelab.bigdata.wasp.core.kafka

import it.agilelab.bigdata.wasp.core.WaspMessage
import it.agilelab.bigdata.wasp.models.configuration.KafkaConfigModel

trait KafkaAdminMessage extends WaspMessage

case class AddTopic(topic: String = NewKafkaAdminActor.topic, partitions: Int = NewKafkaAdminActor.partitions, replicas: Int = NewKafkaAdminActor.replicas) extends KafkaAdminMessage

case class CheckTopic(topic: String = NewKafkaAdminActor.topic) extends KafkaAdminMessage

case class RemoveTopic(topic: String = NewKafkaAdminActor.topic) extends KafkaAdminMessage

case class CheckOrCreateTopic(topic: String = NewKafkaAdminActor.topic, partitions: Int = NewKafkaAdminActor.partitions, replicas: Int = NewKafkaAdminActor.replicas) extends KafkaAdminMessage

case class Initialization(kafkaConfigModel: KafkaConfigModel) extends KafkaAdminMessage