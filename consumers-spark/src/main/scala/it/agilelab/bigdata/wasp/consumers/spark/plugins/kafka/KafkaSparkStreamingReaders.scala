package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkLegacyStreamingReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.??
import it.agilelab.bigdata.wasp.core.bl.TopicBLImp
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{StreamingReaderModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaSparkStructuredStreamingReader extends SparkStructuredStreamingReader with Logging {

  /**
    *
    * Create a Dataframe from a streaming source
    *
    * @param group
    * @param streamingReaderModel
    * @param ss
    * @return
    */
  override def createStructuredStream(
      group: String,
      streamingReaderModel: StreamingReaderModel)(implicit ss: SparkSession): DataFrame = {

    // extract the topic model
    val topic = new TopicBLImp(WaspDB.getDB).getByName(streamingReaderModel.datastoreModelName).get
    
    // get the config
    val kafkaConfig = ConfigManager.getKafkaConfig

    // check or create
    if (??[Boolean](
          WaspSystem.kafkaAdminActor,
          CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

      logger.info(s"Kafka spark options: subscribe: ${topic.name}, " +
                  s"kafka.bootstrap.servers: ${kafkaConfig.connections.map(_.toString).mkString(",")} " +
                  s"kafkaConsumer.pollTimeoutMs: ${kafkaConfig.ingestRateToMills()} " +
                  s"others: ${kafkaConfig.others}")

      // create the stream
      val df: DataFrame = ss.readStream
        .format("kafka")
        .option("subscribe", topic.name)
        .option("kafka.bootstrap.servers", kafkaConfig.connections.map(_.toString).mkString(","))
        .option("kafkaConsumer.pollTimeoutMs", kafkaConfig.ingestRateToMills())
        .options(kafkaConfig.others.map(_.toTupla).toMap)
        .load()

      // prepare the udf
      val byteArrayToJson: Array[Byte] => String = StringToByteArrayUtil.byteArrayToString

      import org.apache.spark.sql.functions._

      val byteArrayToJsonUDF = udf(byteArrayToJson)

      val ret: DataFrame = topic.topicDataType match {
        case "avro" => {
          val rowConverter = AvroToRow(topic.getJsonSchema)
          val encoderForDataColumns = RowEncoder(rowConverter.getSchemaSpark().asInstanceOf[StructType])
          df.select("value").map((r: Row) => {
            val avroByteValue = r.getAs[Array[Byte]](0)
            rowConverter.read(avroByteValue)
          })(encoderForDataColumns)
        }
        case "json" => {
          df.withColumn("value_parsed", byteArrayToJsonUDF(col("value")))
            .drop("value")
            .select(from_json(col("value_parsed"), topic.getDataType).alias("value"))
            .select(col("value.*"))
        }
        case "plaintext" => {
          df
            .select(col("value"))
        }
        case _ => throw new Exception(s"No such topic data type ${topic.topicDataType}")
      }
      logger.debug(s"Kafka reader avro schema: ${new Schema.Parser().parse(topic.getJsonSchema).toString(true)}")
      logger.debug(s"Kafka reader spark schema: ${ret.schema.treeString}")
      ret

    } else {
      val msg = s"Topic not found on Kafka: $topic"
      logger.error(msg)
      throw new Exception(msg)
    }
  }
}

object KafkaSparkLegacyStreamingReader extends SparkLegacyStreamingReader with Logging {

  /**
    * Kafka configuration
    */
  //TODO: check warning (not understood)
  def createStream(group: String, accessType: String, topic: TopicModel)(
      implicit ssc: StreamingContext): DStream[String] = {
    val kafkaConfig = ConfigManager.getKafkaConfig

    val kafkaConfigMap: Map[String, String] = (
      Seq(
        "zookeeper.connect" -> kafkaConfig.zookeeperConnections.toString(),
        "zookeeper.connection.timeout.ms" ->
          kafkaConfig.zookeeperConnections.connections.headOption.flatMap(_.timeout)
            .getOrElse(ConfigManager.getWaspConfig.servicesTimeoutMillis)
            .toString) ++
        kafkaConfig.others.map(_.toTupla)
      )
      .toMap

    if (??[Boolean](
          WaspSystem.kafkaAdminActor,
          CheckOrCreateTopic(topic.name, topic.partitions, topic.replicas))) {

      val receiver: DStream[(String, Array[Byte])] = accessType match {
        case "direct" =>
          KafkaUtils.createDirectStream[String,
                                        Array[Byte],
                                        StringDecoder,
                                        DefaultDecoder](
            ssc,
            kafkaConfigMap + ("group.id" -> group) + ("metadata.broker.list" -> kafkaConfig.connections
              .mkString(",")),
            Set(topic.name)
          )
        case "receiver-based" | _ =>
          KafkaUtils
            .createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
              ssc,
              kafkaConfigMap + ("group.id" -> group),
              Map(topic.name -> 3),
              StorageLevel.MEMORY_AND_DISK_2
            )
      }
      val topicSchema = JsonConverter.toString(topic.schema.asDocument())
      topic.topicDataType match {
        case "avro" =>
          receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2, topicSchema))).map(_._2)
        case "json" | "plaintext" =>
          receiver
            .map(x => (x._1, StringToByteArrayUtil.byteArrayToString(x._2)))
            .map(_._2)
        case _ =>
          receiver.map(x => (x._1, AvroToJsonUtil.avroToJson(x._2, topicSchema))).map(_._2)
      }

    } else {
      val msg = s"Topic not found on Kafka: $topic"
      logger.error(msg)
      throw new Exception(msg)
    }
  }
}