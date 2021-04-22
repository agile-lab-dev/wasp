package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.KafkaWriters._
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.models.{DatastoreModel, MultiTopicModel}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class KafkaSparkStructuredStreamingWriter(topicBL: TopicBL, topicDatastoreModelName: String, ss: SparkSession)
    extends SparkStructuredStreamingWriter
    with Logging {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val tinyKafkaConfig                                 = ConfigManager.getKafkaConfig.toTinyConfig()
    val topicOpt: Option[DatastoreModel] = topicBL.getByName(topicDatastoreModelName)

    val (topicFieldName, topics) = retrieveTopicFieldNameAndTopicModels(topicOpt, topicBL, topicDatastoreModelName)
    val mainTopicModel           = topicOpt.get
    val prototypeTopicModel      = topics.head

    MultiTopicModel
      .areTopicsEqualForWriting(topics)
      .fold(
        s => throw new IllegalArgumentException(s),
        _ => ()
      )

    logger.info(s"Writing with topic model: $mainTopicModel")
    if (mainTopicModel.isInstanceOf[MultiTopicModel]) {
      logger.info(s"""Topic model "${mainTopicModel.name}" is a MultiTopicModel for topics: $topics""")
    }

    askToCheckOrCreateTopics(topics)

    logger.debug(s"Input schema:\n${stream.schema.treeString}")

    val keyFieldName     = prototypeTopicModel.keyFieldName
    val headersFieldName = prototypeTopicModel.headersFieldName
    val valueFieldsNames = prototypeTopicModel.valueFieldsNames

    val finalDf: DataFrame = prepareDfToWrite(
      stream,
      topicFieldName,
      topics,
      prototypeTopicModel,
      keyFieldName,
      headersFieldName,
      valueFieldsNames
    )

    val partialDataStreamWriter = finalDf
      .writeStream
      .format("kafka")

    val partialDataStreamWriterAfterTopicConf =
      if (topicFieldName.isDefined)
        partialDataStreamWriter
      else
        partialDataStreamWriter.option("topic", prototypeTopicModel.name)

    val dataStreamWriterAfterKafkaConfig = addKafkaConf(partialDataStreamWriterAfterTopicConf, tinyKafkaConfig)

    val compressionForKafka = topics.head.topicCompression.kafkaProp

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
