package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import java.nio.charset.StandardCharsets
import java.time.{Clock, Instant}
import java.time.format.DateTimeFormatter
import java.util
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, Future, TimeUnit}

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.metadata.{Metadata, Path}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.WorkerKafkaWriter
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.MetadataUtils
import it.agilelab.bigdata.wasp.core.{TelemetryPipegraph, TelemetryTopicModel, SystemPipegraphs}
import it.agilelab.bigdata.wasp.core.bl.{MlModelBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration.{TinyKafkaConfig, WaspConfigModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}

import scala.util.parsing.json.{JSONFormat, JSONObject}
import scala.util.{Failure, Success, Try}

/**
  * Trait collecting operations to be composed to realize Activation of a [[StructuredStreamingETLModel]]
  */
trait ActivationSteps {

  /**
    * We need a [[StructuredStreamingReader]] able to read from kafka.
    */
  protected val reader: StructuredStreamingReader

  /**
    * We need the plugins map
    */
  protected val plugins: Map[String, WaspConsumersSparkPlugin]

  /**
    * We need a Spark Session
    */
  protected val sparkSession: SparkSession

  /**
    * We need access to machine learning models
    */
  protected val mlModelBl: MlModelBL

  /**
    * We need access to topics
    */
  protected val topicsBl: TopicBL


  /**
    * Performs activation of a [[StructuredStreamingETLModel]] returning the output data frame
    *
    * @param etl The [[StructuredStreamingETLModel]] to activate
    * @return the output dataframe
    */
  protected def activate(etl: StructuredStreamingETLModel): Try[DataFrame] = for {
    streamingSource <- checkOnlyOneStreamingSource(etl).recoverWith {
      case e: Throwable => Failure(new Exception(s"Only one streaming source is allowed in etl ${etl.name}", e))
    }
    structuredInputStream <- createStructuredStreamFromStreamingSource(etl, streamingSource).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot create input from streaming source in etl ${etl.name}", e))
    }
    nonStreamingInputStreams <- createStructuredStreamsFromNonStreamingSources(etl).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot instantiate non streaming sources in etl ${etl.name}", e))
    }
    transformedStream <- applyTransformOrInputIfNoStrategy(etl, structuredInputStream, nonStreamingInputStreams).recoverWith {
      case e: Throwable => Failure(new Exception(s"Failed to apply strategy in etl ${etl.name}", e))
    }

  } yield transformedStream


  /**
    * Checks that [[StructuredStreamingETLModel]] contains only a Streaming source
    *
    * @param etl The etl to check
    * @return The topic model
    */
  private def checkOnlyOneStreamingSource(etl: StructuredStreamingETLModel): Try[TopicModel] = {

    etl.inputs.filter(_.readerType == ReaderType.kafkaReaderType) match {
      //only one kafka reader model
      case Seq(ReaderModel(_, topicName, _)) => retrieveTopic(topicName)
      // more than one kafka reader
      case _ => Failure(new Exception("More than one kafka reader found, only one allowed"))
    }

  }

  /**
    * Retries a [[TopicModel]] by name from DB
    *
    * @param named The name of the topic
    * @return The retrieved [[TopicModel]]
    */
  private def retrieveTopic(named: String) = Try {
    topicsBl.getByName(named)
  } flatMap {
    case Some(topicModel) => Success(topicModel)
    case None => Failure(new Exception(s"Failed to retrieve topic named [$named]"))
  }


  /**
    * Creates structured stream for kafka streaming source
    *
    * @param etl        The etl to activate streaming sources for
    * @param topicModel The model of the topic to read from
    * @return The streaming reader.
    */
  private def createStructuredStreamFromStreamingSource(etl: StructuredStreamingETLModel,
                                                        topicModel: TopicModel): Try[(ReaderKey, DataFrame)] = Try {
    (ReaderKey(TopicModel.readerType, topicModel.name),
      reader.createStructuredStream(etl.group, etl.kafkaAccessType, topicModel)(sparkSession))
  }


  /**
    * Creates structured Streams for non streaming sources
    *
    * @param etl The etl to activate Non streaming sources for
    * @return The created non streaming Sources
    */
  private def createStructuredStreamsFromNonStreamingSources(etl: StructuredStreamingETLModel): Try[Map[ReaderKey, DataFrame]] = {

    def createAnotherStructuredStreamFromNonStreamingSource(previous: Map[ReaderKey, DataFrame],
                                                            readerModel: ReaderModel): Try[Map[ReaderKey, DataFrame]] = {

      def createReader(readerModel: ReaderModel): Try[SparkReader] = Try {
        readerModel match {
          case ReaderModel(name, endpointName, readerType) =>
            plugins.get(readerType.getActualProduct) match {
              case Some(r) => r.getSparkReader(endpointName, name)
              case None => throw new Exception(s"Cannot create Reader, no plugin able to handle [$readerType]")
            }
        }
      }


      def createStructuredStream(reader: SparkReader): Try[(ReaderKey, DataFrame)] = Try {
        (ReaderKey(reader.readerType, reader.name), reader.read(sparkSession.sparkContext))
      }


      for {
        reader <- createReader(readerModel)
        stream <- createStructuredStream(reader)
      } yield previous ++ Map(stream)

    }

    val empty = Try(Map.empty[ReaderKey, DataFrame])


    etl.inputs
      .filterNot(_.readerType == ReaderType.kafkaReaderType)
      .foldLeft(empty) { (previousOutcome, readerModel) =>

        //we update outcome only if createStructuredStream does not blow up
        previousOutcome.flatMap(createAnotherStructuredStreamFromNonStreamingSource(_, readerModel))

      }

  }


  /**
    * Applies the transformation if an input strategy is supplied, if not the input data frame is returned.
    *
    * @param etl                      The etl whose strategy should be applied
    * @param structuredInputStream    The input stream from kafka
    * @param nonStreamingInputStreams The other non streaming DataFrames
    * @return A dataframe with strategy applied or the input DataFrame
    */
  private def applyTransformOrInputIfNoStrategy(etl: StructuredStreamingETLModel, structuredInputStream: (ReaderKey,
    DataFrame), nonStreamingInputStreams: Map[ReaderKey, DataFrame]): Try[DataFrame] = {


    createStrategy(etl) match {
      case Success(Some(strategy)) =>
        applyTransform(structuredInputStream._1,
          structuredInputStream._2,
          nonStreamingInputStreams,
          strategy,
          etl.output.writerType,
          etl)
      case Success(None) =>
        Success(structuredInputStream._2)
      case Failure(reason) =>
        Failure[DataFrame](reason)
    }


  }

  /**
    * Instantiate a strategy if one is configured
    *
    * @param etl The etl to instantiate strategy for
    * @return A try holding an optional strategy
    */
  private def createStrategy(etl: StructuredStreamingETLModel): Try[Option[Strategy]] = {


    def instantiateStrategy(strategyModel: StrategyModel): Try[Strategy] = Try {
      Class.forName(strategyModel.className).newInstance().asInstanceOf[Strategy]
    }

    def configureStrategy(strategyModel: StrategyModel, strategy: Strategy) = Try {

      strategyModel.configurationConfig() match {
        case Some(config) => strategy.configuration = config
        case None => strategy.configuration = ConfigFactory.empty()
      }

      strategy
    }

    def createMlModelBroadcast(models: List[MlModelOnlyInfo]): Try[MlModelsBroadcastDB] = Try {

      val bl = mlModelBl

      object Env {
        val mlModelBL = bl
      }

      val mlModelsDB = new MlModelsDB(Env)

      mlModelsDB.createModelsBroadcast(models)(sparkSession.sparkContext)

    }

    def augmentStrategyWithMlModelsBroadcast(strategy: Strategy, broadcastDB: MlModelsBroadcastDB) = Try {
      strategy.mlModelsBroadcast = broadcastDB
      strategy
    }


    etl.strategy match {
      case Some(strategyModel) =>
        for {
          instantiatedStrategy <- instantiateStrategy(strategyModel)
          configuredStrategy <- configureStrategy(strategyModel, instantiatedStrategy)
          broadcastMlModelDb <- createMlModelBroadcast(etl.mlModels)
          augmented <- augmentStrategyWithMlModelsBroadcast(configuredStrategy, broadcastMlModelDb).map(Some(_))
        } yield augmented
      case None =>
        Success[Option[Strategy]](None)
    }


  }

  /**
    * Applies strategy and handles metadata collection for telemetry purposes.
    *
    * @param readerKey    The key to place the resulting stream in the map passed to the strategy
    * @param stream       The input stream coming from kafka
    * @param dataStoreDFs The data frames representing non streaming data stores
    * @param strategy     The strategy to be applied
    * @param writerType   The type of the output writer, will be used to properly handle metadata schema
    * @param etl          The etl model
    * @return A Try representing the application of the strategy as a new DataFrame
    */
  private def applyTransform(readerKey: ReaderKey,
                             stream: DataFrame,
                             dataStoreDFs: Map[ReaderKey, DataFrame],
                             strategy: Strategy,
                             writerType: WriterType,
                             etl: StructuredStreamingETLModel): Try[DataFrame] = Try {

    val config = ConfigManager.getKafkaConfig.toTinyConfig()

    val inputStreamWithEnterMetadata = MetadataOps.sendLatencyMessage(MetadataOps.enter(etl.name, stream), config)

    val strategyInputStreams = dataStoreDFs + (readerKey -> inputStreamWithEnterMetadata)

    val strategyOutputStream = strategy.transform(strategyInputStreams)

    val strategyOutputStreamWithExitMetadata = MetadataOps.sendLatencyMessage(MetadataOps.exit(etl.name,
      strategyOutputStream),config)

    writerType.getActualProduct match {
      case Datastores.kafkaProduct => strategyOutputStreamWithExitMetadata
      case Datastores.hbaseProduct => strategyOutputStreamWithExitMetadata
      case Datastores.rawProduct => strategyOutputStreamWithExitMetadata
      case Datastores.consoleProduct => strategyOutputStreamWithExitMetadata
      case _ =>
        if (strategyOutputStreamWithExitMetadata.columns.contains("metadata")) {
          strategyOutputStreamWithExitMetadata.select(MetadataUtils.flatMetadataSchema(strategyOutputStreamWithExitMetadata.schema, None): _*)
        }
        else
          strategyOutputStreamWithExitMetadata
    }
  }
}

object MetadataOps {

  def enter(etlName: String, stream: DataFrame): DataFrame =
    on(etlName.replace(' ', '-') + "-enter", stream)

  def exit(etlName: String, stream: DataFrame): DataFrame =
    on(etlName.replace(' ', '-') + "-exit", stream)

  private def on(path: String, stream: DataFrame): DataFrame =
    if (stream.columns.contains("metadata")) {

      val originalColumnsOrder = stream.columns

      val updateFunction = MetadataOps.updateMetadata(path)

      stream.withColumn("metadata_new",
        updateFunction(
          col("metadata.id"),
          col("metadata.sourceId"),
          col("metadata.arrivalTimestamp"),
          col("metadata.lastSeenTimestamp"),
          col("metadata.path")
        ))
        .drop("metadata")
        .withColumnRenamed("metadata_new", "metadata")
        .select(originalColumnsOrder.head, originalColumnsOrder.tail: _*)
    }
    else {
      stream
    }


  def updateMetadata(path: String): UserDefinedFunction = udf {
    (mId: String, mSourceId: String, mArrivalTimestamp: Long, _: Long, mPath: Seq[Row]) => {

      val nowInstant = Clock.systemUTC().instant()
      val now = nowInstant.toEpochMilli
      val oldPaths = mPath.map(r => Path(r))
      val newPaths = (oldPaths :+ Path(path, now)).toArray


      Metadata(mId,
        mSourceId,
        mArrivalTimestamp,
        now,
        newPaths)

    }
  }


  def sendLatencyMessage(stream: DataFrame, kafkaConfig: TinyKafkaConfig): DataFrame =

    if (stream.columns.contains("metadata")) {


      val connectionString = kafkaConfig.connections.map{
        conn => s"${conn.host}:${conn.port}"
      }.mkString(",")

      val props = new Properties()
      props.put("bootstrap.servers", connectionString)
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("batch.size", "1048576")
      props.put("acks", "0")
      kafkaConfig.others.filterNot(_.key=="acks").foreach(entry => props.put(entry.key, entry.value))

      implicit val rowEncoder: Encoder[Row] = RowEncoder(stream.schema)

      stream.mapPartitions { partition: Iterator[Row] =>



        val writer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer[Array[Byte], Array[Byte]](props)
        TaskContext.get().addTaskCompletionListener(_ => writer.close())

        val counter = new AtomicInteger()

        partition.map { row =>

          if(counter.getAndIncrement() % 1 == 0) {

            val metadata = row.getStruct(row.fieldIndex("metadata"))

            val pathField = metadata.fieldIndex("path")

            val messageId = metadata.getString(metadata.fieldIndex("id"))

            val sourceId = metadata.getString(metadata.fieldIndex("sourceId"))

            val arrivalTimestamp = metadata.getLong(metadata.fieldIndex("arrivalTimestamp"))

            val path = Path(sourceId,arrivalTimestamp) +: metadata.getSeq[Row](pathField).map(Path.apply)

            val lastTwoHops = path.takeRight(2)

            val latency = lastTwoHops(1).ts - lastTwoHops(0).ts

            val collectionTimeAsString = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(lastTwoHops(1).ts))

            val compositeSourceId = path.map(_.name.replace(' ', '-')).mkString("/")


            val json = JSONObject(Map("messageId" -> messageId,
                                      "sourceId" -> compositeSourceId,
                                      "metric" -> "latencyMs",
                                      "value" -> latency,
                                      "timestamp" -> collectionTimeAsString)).toString(JSONFormat.defaultFormatter)


            val topic = SystemPipegraphs.telemetryTopic.name

            val record = new ProducerRecord[Array[Byte], Array[Byte]](topic,
                                                                      messageId.getBytes(StandardCharsets.UTF_8),
                                                                      json.getBytes(StandardCharsets.UTF_8))
            writer.send(record)

          }

          row
        }

      }
    } else {
      stream
    }

}