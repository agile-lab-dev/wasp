package it.agilelab.bigdata.wasp.spark.plugins.telemetry

import java.nio.charset.StandardCharsets
import java.util.Base64


import scala.concurrent.duration.Duration
import scala.util.parsing.json.JSON

trait ConfigurationSupport {

  lazy val configuration: TelemetryPluginConfiguration = {

    val interval = Duration(System.getProperty("wasp.plugin.telemetry.collection-interval", "10s"))
    TelemetryPluginConfiguration(interval, TelemetryMetadataProducerConfig(telemetry, kafka))
  }

  private lazy val telemetry = parseTelemetry()

  private lazy val kafka = parseKafka()

  @com.github.ghik.silencer.silent("deprecated")
  private def parseTelemetry() = {
    val telemetryConfigJSON = new String(Base64.getUrlDecoder.decode(System.getProperty("wasp.plugin.telemetry.topic")), StandardCharsets.UTF_8)
    val telemetryConfig = JSON.parseFull(telemetryConfigJSON)

    val tt = telemetryConfig.get.asInstanceOf[Map[String, Any]]

    val topicName = tt("topicName").asInstanceOf[String]
    val partitions = tt("partitions").asInstanceOf[Double].toInt
    val replica = tt("replica").asInstanceOf[Double].toInt
    val kafkaSettings = tt.get("kafkaSettings").map(_.asInstanceOf[Seq[Map[String, Any]]]).map(_.map { s =>
      (s("key").asInstanceOf[String], s("value").asInstanceOf[String])
    }).getOrElse(Seq.empty)


    val jmx = tt.get("jmx").map(_.asInstanceOf[Seq[Map[String, Any]]]).map(_.map { s =>
      TelemetryPluginJMXTelemetryConfigModel(query = s("query").asInstanceOf[String],
        metricGroupAttribute = s("metricGroupAttribute").asInstanceOf[String],
        sourceIdAttribute = s("sourceIdAttribute").asInstanceOf[String],
        metricGroupFallback = s("metricGroupFallback").asInstanceOf[String],
        sourceIdFallback = s("metricGroupFallback").asInstanceOf[String])
    }).getOrElse(Seq.empty)


    TelemetryPluginTopicConfigModel(topicName = topicName,
      partitions = partitions,
      replica = replica,
      kafkaSettings = kafkaSettings,
      jmx = jmx)
  }

  @com.github.ghik.silencer.silent("deprecated")
  private def parseKafka() = {
    val kafkaTinyConfigJSON = new String(Base64.getUrlDecoder.decode(System.getProperty("wasp.plugin.telemetry.kafka")), StandardCharsets.UTF_8)
    val kafkaTinyConfig = JSON.parseFull(kafkaTinyConfigJSON)
    val kk = kafkaTinyConfig.get.asInstanceOf[Map[String, Any]]

    val batch_send_size = kk("batch_send_size").asInstanceOf[Double].toInt
    val acks = kk("acks").asInstanceOf[String]
    val default_encoder = kk("default_encoder").asInstanceOf[String]
    val encoder_fqcn = kk("encoder_fqcn").asInstanceOf[String]
    val partitioner_fqcn = kk("partitioner_fqcn").asInstanceOf[String]

    val others = kk.get("others").map(_.asInstanceOf[Seq[Map[String, Any]]]).map(_.map { s =>
      (s("key").asInstanceOf[String], s("value").asInstanceOf[String])
    }).getOrElse(Seq.empty)

    val connections = kk("connections").asInstanceOf[Seq[Map[String, Any]]].map { conn =>
      TelemetryPluginConnectionConfig(
        protocol = conn("protocol").asInstanceOf[String],
        host = conn("host").asInstanceOf[String],
        port = conn("port").asInstanceOf[Double].toInt,
        timeout = conn.get("timeout").map(_.asInstanceOf[Double].toLong),
        metadata = conn.get("metadata").map(_.asInstanceOf[Map[String, String]]))
    }

    TelemetryPluginKafkaConfig(connections, batch_send_size, acks, default_encoder, encoder_fqcn, partitioner_fqcn, others)
  }
}
