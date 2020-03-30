package it.agilelab.bigdata.wasp.producers.metrics.kafka.throughput

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.utils.ConfUtils._

case class KafkaThroughputConfig(topicToCheck: String,
                                 triggerInterval: Long,
                                 windowSize: Long,
                                 sendMessageEvery: Int)

object KafkaThroughputConfig {

  def fromConfig(conf: Config): Either[String, KafkaThroughputConfig] = {
    for {
      topic <- getString(conf, "topicName")
      triggerIntervalMs <- getLong(conf, "triggerIntervalMs")
      windowSize <- getLong(conf, "windowSizeMs")
      sendMessageEvery <- getInt(conf, "sendMessageEvery")
    } yield KafkaThroughputConfig(topic, triggerIntervalMs, windowSize, sendMessageEvery)
  }
}