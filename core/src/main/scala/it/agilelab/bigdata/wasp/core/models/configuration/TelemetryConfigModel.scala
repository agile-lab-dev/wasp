package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model

/**
  * Configuration model for telemetry functionality.
  *
  * @author andreaL
  */

case class TelemetryTopicConfigModel(topicName: String,
                                     partitions: Int,
                                     replica: Int,
                                     kafkaSettings: Seq[KafkaEntryConfig])

case class TelemetryConfigModel(val name: String,
                                writer: String,
                                sampleOneMessageEvery: Int,
                                telemetryTopicConfigModel: TelemetryTopicConfigModel) extends Model {

}

