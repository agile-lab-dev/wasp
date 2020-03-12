package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.KafkaWriters._
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkBatchWriter
import it.agilelab.bigdata.wasp.core.bl.TopicBL
import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.configuration.{KafkaEntryConfig, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.core.models.{DatastoreModel, MultiTopicModel, TopicCompression}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

class KafkaBatchWriter(topicBL: TopicBL,
                       topicDatastoreModelName: String,
                       ss: SparkSession) extends SparkBatchWriter with Logging {

  override def write(df: DataFrame): Unit = {

    val tinyKafkaConfig = ConfigManager.getKafkaConfig.toTinyConfig()
    val topicOpt: Option[DatastoreModel[TopicCategory]] = topicBL.getByName(topicDatastoreModelName)

    val (topicFieldName, topics) = retrieveTopicFieldNameAndTopicModels(topicOpt, topicBL, topicDatastoreModelName)
    val mainTopicModel = topicOpt.get
    val prototypeTopicModel = topics.head

    MultiTopicModel.validateTopicModels(topics)

    logger.info(s"Writing with topic model: $mainTopicModel")
    if (mainTopicModel.isInstanceOf[MultiTopicModel]) {
      logger.info(s"""Topic model "${mainTopicModel.name}" is a MultiTopicModel for topics: $topics""")
    }

    askToCheckOrCreateTopics(topics)

    logger.debug(s"Input schema:\n${df.schema.treeString}")

    val keyFieldName = prototypeTopicModel.keyFieldName
    val headersFieldName = prototypeTopicModel.headersFieldName
    val valueFieldsNames = prototypeTopicModel.valueFieldsNames

    val finalDf: DataFrame = prepareDfToWrite(df,
      topicFieldName,
      topics,
      prototypeTopicModel,
      keyFieldName,
      headersFieldName,
      valueFieldsNames)

    val partialDfWriter = finalDf
      .write
      .format("kafka")

    val partialDfWriterAfterTopicConf =
      if (topicFieldName.isDefined)
        partialDfWriter
      else
        partialDfWriter.option("topic", prototypeTopicModel.name)

    val dataframeWriterAfterKafkaConfig = addKafkaConf(partialDfWriterAfterTopicConf, tinyKafkaConfig)

    val compressionForKafka = TopicCompression.asString.applyOrElse(topics.head.topicCompression, {
      notMatched: TopicCompression =>
        throw new Exception(s"$notMatched compression is not supported by kafka writer")
    })

    val finalDataframeWriterAfterCompression = dataframeWriterAfterKafkaConfig
      .option("kafka.compression.type", compressionForKafka)

    finalDataframeWriterAfterCompression.save()
  }

  private def addKafkaConf(dsw: DataFrameWriter[Row], tkc: TinyKafkaConfig): DataFrameWriter[Row] = {

    val connectionString = tkc.connections.map {
      conn => s"${conn.host}:${conn.port}"
    }.mkString(",")

    val kafkaConfigMap: Seq[KafkaEntryConfig] = tkc.others

    dsw
      .option("kafka.bootstrap.servers", connectionString)
      .option("kafka.partitioner.class", tkc.partitioner_fqcn)
      .option("kafka.batch.size", tkc.batch_send_size.toString)
      .option("kafka.acks", tkc.acks)
      .options(kafkaConfigMap.map(_.toTupla).toMap)
  }
}