package it.agilelab.bigdata.wasp.core.kafka

import java.util
import java.util.Properties

import akka.actor.ActorRef
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.KafkaConfigModel
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.convert.decorateAsScala._

class WaspKafkaReader[K, V](consumerConfig: Properties) extends Logging {

  def this(conf: KafkaConfigModel, group: String, zookeeper: String) = this(WaspKafkaReader.createConfig(
    conf.connections.map(x => x.toString).toSet, group, zookeeper))

  logger.info(s"consumerConfig $consumerConfig")

  private val consumer = new KafkaConsumer[String, String](consumerConfig)

  def subscribe(topic: String, listener: ActorRef) = {

    consumer.subscribe(util.Arrays.asList(topic))

    val thread = new Thread {
      override def run {
        while (true) {
          val records: ConsumerRecords[String, String] = consumer.poll(100)
          for (rec <- records.asScala) {
            listener ! (topic, rec.value())
          }
        }
      }
    }

    thread.start()

  }

  def close(): Unit = consumer.close()

}


object WaspKafkaReader {

  def createConfig(brokers: Set[String], group: String, zookeper: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers.mkString(","))
    props.put("zookeeper.connect", zookeper)
    props.put("group.id", group)
    props.put("session.timeout.ms", "1000")
    props.put("auto.commit.enable", "true")
    props.put("auto.commit.interval.ms", "100")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("partition.assignment.strategy", "range")
    props
  }

}
