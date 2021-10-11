package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.TopicModelUtils.retrieveKafkaTopicSettings
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class KafkaSparkStructuredStreamingWriter(topicBL: TopicBL, topicDatastoreModelName: String, ss: SparkSession)
    extends SparkStructuredStreamingWriter
    with Logging {

  override def write(df: DataFrame): DataStreamWriter[Row] = {
    val settings = retrieveKafkaTopicSettings(topicBL, topicDatastoreModelName)

    val finalDf: DataFrame =
      KafkaWriters.convertDataframe(
        df,
        settings.topicFieldName,
        settings.topics,
        settings.mainTopicModel,
        settings.darwinConf
      )

    val partialDataStreamWriter = finalDf.writeStream
      .format("kafka")

    val partialDataStreamWriterAfterTopicConf =
      if (settings.isMultiTopic)
        partialDataStreamWriter
      else
        partialDataStreamWriter.option("topic", settings.topics.head.name)

    val dataStreamWriterAfterKafkaConfig = addKafkaConf(partialDataStreamWriterAfterTopicConf, settings.tinyKafkaConfig)

    val compressionForKafka = settings.topics.head.topicCompression.kafkaProp

    val finalDataStreamWriterAfterCompression = dataStreamWriterAfterKafkaConfig
      .option("kafka.compression.type", compressionForKafka)

    finalDataStreamWriterAfterCompression
  }

  private def addKafkaConf(dsw: DataStreamWriter[Row], tkc: TinyKafkaConfig): DataStreamWriter[Row] = {

    val connectionString = tkc.connections
      .map { conn =>
        s"${conn.host}:${conn.port}"
      }
      .mkString(",")

    val kafkaConfigMap: Seq[KafkaEntryConfig] = tkc.others

    dsw
      .option("kafka.bootstrap.servers", connectionString)
      .option("kafka.partitioner.class", tkc.partitioner_fqcn)
      .option("kafka.batch.size", tkc.batch_send_size.toString)
      .option("kafka.acks", tkc.acks)
      .options(kafkaConfigMap.map(_.toTupla).toMap)
  }
}
