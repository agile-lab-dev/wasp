package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.TopicModelUtils.retrieveKafkaTopicSettings
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

class KafkaBatchWriter(topicBL: TopicBL, topicDatastoreModelName: String, ss: SparkSession)
    extends SparkBatchWriter
    with Logging {

  override def write(df: DataFrame): Unit = {

    val settings = retrieveKafkaTopicSettings(topicBL, topicDatastoreModelName)

    val finalDf: DataFrame = KafkaWriters.convertDataframe(
      df,
      settings.topicFieldName,
      settings.topics,
      settings.mainTopicModel,
      settings.darwinConf
    )

    val partialDfWriter = finalDf.write.format("kafka")

    val partialDfWriterAfterTopicConf =
      if (settings.isMultiTopic)
        partialDfWriter
      else
        partialDfWriter.option("topic", settings.topicsToWrite.head.name)

    val dataframeWriterAfterKafkaConfig = addKafkaConf(partialDfWriterAfterTopicConf, settings.tinyKafkaConfig)

    val compressionForKafka = settings.topicsToWrite.head.topicCompression.kafkaProp

    val finalDataframeWriterAfterCompression = dataframeWriterAfterKafkaConfig
      .option("kafka.compression.type", compressionForKafka)

    finalDataframeWriterAfterCompression.save()
  }

  private def addKafkaConf(dsw: DataFrameWriter[Row], tkc: TinyKafkaConfig): DataFrameWriter[Row] = {

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
