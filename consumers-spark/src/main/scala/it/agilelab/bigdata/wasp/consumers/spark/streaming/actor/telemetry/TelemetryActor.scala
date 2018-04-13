package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.telemetry

import java.nio.charset.StandardCharsets
import java.util.{Properties, UUID}

import akka.actor.{Actor, Props}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MonitorOutcome
import it.agilelab.bigdata.wasp.core.SystemPipegraphs
import it.agilelab.bigdata.wasp.core.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.spark.sql.streaming.StreamingQueryProgress

import scala.collection.JavaConverters._
import scala.util.parsing.json.{JSONFormat, JSONObject}

class TelemetryActor private (kafkaConnectionString: String, kafkaConfig: TinyKafkaConfig) extends Actor {


  private var writer: Producer[Array[Byte], Array[Byte]] = _

  override def preStart(): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", kafkaConnectionString)
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("batch.size", "1048576")
    props.put("acks", "0")

    val notOverridableKeys = props.keySet.asScala

    kafkaConfig.others.filterNot(notOverridableKeys.contains(_)).foreach {
      case KafkaEntryConfig(key, value) => props.put(key, value)
    }

    writer = new KafkaProducer[Array[Byte], Array[Byte]](props)
  }


  override def postStop(): Unit = {
    writer.close()
  }


  override def receive: Receive = {
    case MonitorOutcome(_, _, Some(progress), _) => send(progress)
    case _ =>

  }


  private def toMessage(map: Map[String, Any])= JSONObject(map).toString(JSONFormat.defaultFormatter)

  private def metric(header: Map[String, Any], metric: String, value:Double) =
    header + ("metric" -> metric) + ("value" -> value)


  private def isValidMetric(metric: Map[String,Any]) = {
    val value = metric("value").asInstanceOf[Double]

    !value.isNaN && !value.isInfinity
  }

  private def send(progress: StreamingQueryProgress) : Unit = {


    val header = Map("messageId" -> progress.id.toString,
                     "sourceId" -> progress.name,
                     "timestamp" -> progress.timestamp)



    val durationMs = progress.durationMs.asScala.map {
                      case (key, value) => metric(header, s"$key-durationMs", value.toDouble)
                     }.toSeq

    val metrics = durationMs :+
                  metric(header, "numberOfInputRows", progress.numInputRows) :+
                  metric(header, "inputRowsPerSecond", progress.inputRowsPerSecond) :+
                  metric(header, "processedRowsPerSecond", progress.processedRowsPerSecond)


    metrics.filter(isValidMetric)
           .map(toMessage)
           .foreach(send(UUID.randomUUID().toString, _))



  }


  private def send(key: String, message: String) : Unit = {

    val topic = SystemPipegraphs.telemetryTopic.name

    val record = new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      key.getBytes(StandardCharsets.UTF_8),
      message.getBytes(StandardCharsets.UTF_8)
    )

    writer.send(record)
  }
}


object TelemetryActor {

  def props(kafkaConnectionString:String , kafkaConfig: TinyKafkaConfig): Props =
    Props(new TelemetryActor(kafkaConnectionString,
                             kafkaConfig))

}