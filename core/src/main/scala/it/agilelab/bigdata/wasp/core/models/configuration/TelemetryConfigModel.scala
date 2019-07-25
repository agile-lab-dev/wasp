package it.agilelab.bigdata.wasp.core.models.configuration

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
  * Configuration model for telemetry functionality.
  *
  * @author andreaL
  */

case class JMXTelemetryConfigModel(query: String, metricGroupAttribute: String, sourceIdAttribute: String, metricGroupFallback: String = "unknown", sourceIdFallback: String = "unknown")

case class TelemetryTopicConfigModel(topicName: String,
                                     partitions: Int,
                                     replica: Int,
                                     kafkaSettings: Seq[KafkaEntryConfig],
                                     jmx: Seq[JMXTelemetryConfigModel])

case class TelemetryConfigModel(val name: String,
                                writer: String,
                                sampleOneMessageEvery: Int,
                                telemetryTopicConfigModel: TelemetryTopicConfigModel) extends Model {

}

object TelemetryTopicConfigModelMessageFormat extends SprayJsonSupport with DefaultJsonProtocol {

  implicit lazy val JMXTelemetryConfigModelFormat: RootJsonFormat[JMXTelemetryConfigModel] = jsonFormat5(JMXTelemetryConfigModel.apply)
  implicit lazy val telemetryTopicConfigModelFormat: RootJsonFormat[TelemetryTopicConfigModel] = jsonFormat5(TelemetryTopicConfigModel.apply)
  implicit lazy val kafkaEntryConfigModelFormat: RootJsonFormat[KafkaEntryConfig] = jsonFormat2(KafkaEntryConfig.apply)
  implicit lazy val tinyKafkaConfigFormat: RootJsonFormat[TinyKafkaConfig] = jsonFormat7(TinyKafkaConfig.apply)
  implicit lazy val connectionConfigFormat: RootJsonFormat[ConnectionConfig] = jsonFormat5(ConnectionConfig.apply)

}