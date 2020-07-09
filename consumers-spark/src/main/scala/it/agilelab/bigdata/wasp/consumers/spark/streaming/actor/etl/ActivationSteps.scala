package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant}
import java.util.Properties
import java.util.concurrent.Future

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.metadata.{Metadata, Path}
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkStructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{FreeCodeStrategy, ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.ActivationSteps.{StaticReaderFactory, StreamingReaderFactory}
import it.agilelab.bigdata.wasp.consumers.spark.utils.MetadataUtils
import it.agilelab.bigdata.wasp.repository.core.bl.{FreeCodeBL, MlModelBL, ProcessGroupBL, TopicBL}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct._
import it.agilelab.bigdata.wasp.models.configuration.{KafkaEntryConfig, TelemetryConfigModel, TinyKafkaConfig}
import it.agilelab.bigdata.wasp.models.{MlModelOnlyInfo, ReaderModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, TopicModel}
import org.apache.kafka.clients.producer._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import spray.json._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Trait collecting operations to be composed to realize Activation of a [[StructuredStreamingETLModel]]
  */
trait ActivationSteps {

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
    * We need access to freeCodes
    */
  protected val freeCodeBL: FreeCodeBL

  /**
    * We need access to freeCodes
    */
  protected val processGroupBL: ProcessGroupBL

  /**
    * We need a streaming reader factory
    */
  protected val streamingReaderFactory: StreamingReaderFactory

  /**
    * We need a static reader factory
    */
  protected val staticReaderFactory: StaticReaderFactory

  /**
    * Performs activation of a [[StructuredStreamingETLModel]] returning the output data frame
    *
    * @param etl The [[StructuredStreamingETLModel]] to activate
    * @return the output dataframe
    */
  protected def activate(etl: StructuredStreamingETLModel): Try[DataFrame] =
    for {
      streamingDataFrame <- createStreamingDataFrameFromStreamingSource(etl, etl.streamingInput).recoverWith {
                             case e: Throwable =>
                               Failure(
                                 new Exception(s"Cannot create input from streaming source in etl ${etl.name}", e)
                               )
                           }
      staticDataFrames <- createStaticDataFramesFromStaticSources(etl).recoverWith {
                           case e: Throwable =>
                             Failure(new Exception(s"Cannot instantiate static sources in etl ${etl.name}", e))
                         }
      transformedStream <- applyTransformOrInputIfNoStrategy(etl, streamingDataFrame, staticDataFrames).recoverWith {
                            case e: Throwable =>
                              Failure(new Exception(s"Failed to apply strategy in etl ${etl.name}", e))
                          }

    } yield transformedStream

  /**
    * Retries a [[TopicModel]] by name from DB
    *
    * @param named The name of the topic
    * @return The retrieved [[TopicModel]]
    */
  private def retrieveTopic(named: String) =
    Try {
      topicsBl.getByName(named)
    } flatMap {
      case Some(topicModel) => Success(topicModel)
      case None             => Failure(new Exception(s"Failed to retrieve topic named [$named]"))
    }

  /**
    * Creates structured stream for a streaming source
    *
    * @param etl                  The etl to activate streaming sources for
    * @param streamingReaderModel The model of the streaming source to read from
    * @return The streaming reader.
    */
  private def createStreamingDataFrameFromStreamingSource(
      etl: StructuredStreamingETLModel,
      streamingReaderModel: StreamingReaderModel
  ): Try[(ReaderKey, DataFrame)] = Try {
    val maybeReader = streamingReaderFactory(etl, streamingReaderModel, sparkSession)
    val streamingDataFrame = maybeReader match {
      case Some(reader) => reader.createStructuredStream(etl, streamingReaderModel)(sparkSession)
      case None =>
        val datastoreProduct = streamingReaderModel.datastoreProduct
        throw new Exception(
          s"""Cannot create streaming reader, no plugin able to handle datastore product "$datastoreProduct" found"""
        )
    }
    (ReaderKey(streamingReaderModel.datastoreProduct.categoryName, streamingReaderModel.name), streamingDataFrame)
  }

  /**
    * Creates structured Streams for non streaming sources
    *
    * @param etl The etl to activate Non streaming sources for
    * @return The created non streaming Sources
    */
  private def createStaticDataFramesFromStaticSources(
      etl: StructuredStreamingETLModel
  ): Try[Map[ReaderKey, DataFrame]] = {

    def createAnotherStaticDataFrameFromStaticSource(
        previous: Map[ReaderKey, DataFrame],
        readerModel: ReaderModel
    ): Try[Map[ReaderKey, DataFrame]] = {

      def createReader(readerModel: ReaderModel): Try[SparkBatchReader] = Try {
        val maybeReader = staticReaderFactory(etl, readerModel, sparkSession)
        maybeReader match {
          case Some(reader) => reader
          case None =>
            val datastoreProduct = readerModel.datastoreProduct
            throw new Exception(
              s"""Cannot create static reader, no plugin able to handle datastore product "$datastoreProduct" found"""
            )
        }
      }

      def createStructuredStream(reader: SparkBatchReader): Try[(ReaderKey, DataFrame)] = Try {
        (ReaderKey(reader.readerType, reader.name), reader.read(sparkSession.sparkContext))
      }

      for {
        reader <- createReader(readerModel)
        stream <- createStructuredStream(reader)
      } yield previous ++ Map(stream)

    }

    val empty = Try(Map.empty[ReaderKey, DataFrame])

    etl.staticInputs
      .foldLeft(empty) { (previousOutcome, readerModel) =>
        //we update outcome only if createStructuredStream does not blow up
        previousOutcome.flatMap(createAnotherStaticDataFrameFromStaticSource(_, readerModel))

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
  private def applyTransformOrInputIfNoStrategy(
      etl: StructuredStreamingETLModel,
      structuredInputStream: (ReaderKey, DataFrame),
      nonStreamingInputStreams: Map[ReaderKey, DataFrame]
  ): Try[DataFrame] = {

    createStrategy(etl) match {
      case Success(Some(strategy)) =>
        applyTransform(
          structuredInputStream._1,
          structuredInputStream._2,
          nonStreamingInputStreams,
          strategy,
          etl.streamingOutput.datastoreProduct,
          etl
        )
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
  protected def createStrategy(etl: StructuredStreamingETLModel): Try[Option[Strategy]] = {

    def instantiateStrategy(strategyModel: StrategyModel): Try[Strategy] = Try {
      val conf = strategyModel.configurationConfig().getOrElse(ConfigFactory.empty())

      if (strategyModel.className.equals(classOf[FreeCodeStrategy].getName)) {
        require(conf.hasPath("name"), s"Configuration for FreeCodeStrategy isn't defined.")
        val freeCodeOption = freeCodeBL.getByName(conf.getString("name"))
        require(freeCodeOption.isDefined, s"Free code with name ${strategyModel.configuration.get} not found.")
        val strategy = new FreeCodeStrategy(freeCodeOption.get.code)
        strategy.configuration = conf
        strategy
      } else {

        if (strategyModel.className.equals("it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiStrategy")) {

          require(
            conf.hasPath("nifi.process-group-id"),
            s"Nifi.process-group-id Configuration for NifiStrategy isn't defined."
          )

          val newStrategy = Class.forName(strategyModel.className).newInstance().asInstanceOf[Strategy]
          newStrategy.configuration = processGroupBL
            .getById(conf.getString("nifi.process-group-id"))
            .map(processGroup => conf.withValue("nifi.flow", ConfigValueFactory.fromAnyRef(processGroup.content.toJson))
            )
            .getOrElse(conf)
          newStrategy
        } else {
          val strategy = Class.forName(strategyModel.className).newInstance().asInstanceOf[Strategy]
          strategy.configuration = conf
          strategy
        }
      }
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
          configuredStrategy <- instantiateStrategy(strategyModel)
          broadcastMlModelDb <- createMlModelBroadcast(etl.mlModels)
          augmented          <- augmentStrategyWithMlModelsBroadcast(configuredStrategy, broadcastMlModelDb).map(Some(_))
        } yield augmented
      case None =>
        Success[Option[Strategy]](None)
    }

  }

  /**
    * Applies strategy and handles metadata collection for telemetry purposes.
    *
    * @param readerKey        The key to place the resulting stream in the map passed to the strategy
    * @param stream           The input stream coming from kafka
    * @param dataStoreDFs     The data frames representing non streaming data stores
    * @param strategy         The strategy to be applied
    * @param datastoreProduct The type of the output datastore, will be used to properly handle metadata schema
    * @param etl              The etl model
    * @return A Try representing the application of the strategy as a new DataFrame
    */
  private def applyTransform(
      readerKey: ReaderKey,
      stream: DataFrame,
      dataStoreDFs: Map[ReaderKey, DataFrame],
      strategy: Strategy,
      datastoreProduct: DatastoreProduct,
      etl: StructuredStreamingETLModel
  ): Try[DataFrame] = Try {

    val config =
      TelemetryMetadataProducerConfig(ConfigManager.getKafkaConfig.toTinyConfig(), ConfigManager.getTelemetryConfig)

    val keyDefaultOneMessageEveryKey   = "wasp.telemetry.latency.sample-one-message-every"
    val valueDefaultOneMessageEveryKey = ConfigManager.getTelemetryConfig.sampleOneMessageEvery

    val defaultConfiguration =
      ConfigFactory.parseString(s"$keyDefaultOneMessageEveryKey=$valueDefaultOneMessageEveryKey")

    val sampleOneMessageEveryValue =
      strategy.configuration.withFallback(defaultConfiguration).getInt(keyDefaultOneMessageEveryKey)

    val saneSampleOneMessageEvery = if (sampleOneMessageEveryValue < 1) 1 else sampleOneMessageEveryValue

    val dropMetadataDefault = ConfigFactory.parseString(s"dropMetadata=false")
    val dropMetadataColumn = etl.strategy
      .flatMap(strategy =>
        strategy.configurationConfig().map(_.withFallback(dropMetadataDefault).getBoolean("dropMetadata"))
      )
      .getOrElse(false)

    val inputStreamWithEnterMetadata =
      MetadataOps.sendLatencyMessage(MetadataOps.enter(etl.name, stream), config, saneSampleOneMessageEvery)

    val strategyInputStreams = dataStoreDFs + (readerKey -> inputStreamWithEnterMetadata)

    val strategyOutputStream = strategy.transform(strategyInputStreams)

    val strategyOutputStreamWithExitMetadata = MetadataOps
      .sendLatencyMessage(MetadataOps.exit(etl.name, strategyOutputStream), config, saneSampleOneMessageEvery)

    val cleanOutputStream: DataFrame = dropMetadataColumn match {
      case true =>
        if (strategyOutputStreamWithExitMetadata.columns.contains("metadata"))
          strategyOutputStreamWithExitMetadata.drop("metadata")
        else strategyOutputStreamWithExitMetadata
      case false => strategyOutputStreamWithExitMetadata
    }

    // TODO maybe we should match on categories instead
    datastoreProduct match {
      case KafkaProduct   => cleanOutputStream
      case HBaseProduct   => cleanOutputStream
      case RawProduct     => cleanOutputStream
      case ConsoleProduct => cleanOutputStream
      case _ =>
        if (cleanOutputStream.columns.contains("metadata")) {
          cleanOutputStream.select(MetadataUtils.flatMetadataSchema(cleanOutputStream.schema, None): _*)
        } else
          cleanOutputStream
    }
  }
}

object ActivationSteps {

  /**
    * A function able to go from a [[StructuredStreamingETLModel]]] and a [[StreamingReaderModel]] to an [[Option]]
    * of [[SparkStructuredStreamingReader]].
    *
    * The goal of this type is to abstract out the concrete implementation of this computation.
    */
  type StreamingReaderFactory =
    (StructuredStreamingETLModel, StreamingReaderModel, SparkSession) => Option[SparkStructuredStreamingReader]

  /**
    * A function able to go from a [[StructuredStreamingETLModel]]] and a [[ReaderModel]] to an [[Option]] of
    * [[SparkBatchReader]].
    *
    * The goal of this type is to abstract out the concrete implementation of this computation.
    */
  type StaticReaderFactory = (StructuredStreamingETLModel, ReaderModel, SparkSession) => Option[SparkBatchReader]
}

case class TelemetryMetadataProducerConfig(global: TinyKafkaConfig, telemetry: TelemetryConfigModel)

object TelemetryMetadataProducer {

  @transient private lazy val cache
      : LoadingCache[TelemetryMetadataProducerConfig, KafkaProducer[Array[Byte], Array[Byte]]] = CacheBuilder
    .newBuilder()
    .build(load())

  def send(kafkaConfig: TelemetryMetadataProducerConfig, key: String, value: JsValue): Future[RecordMetadata] = {
    val topicName = TopicModel.name(kafkaConfig.telemetry.telemetryTopicConfigModel.topicName)
    val record = new ProducerRecord[Array[Byte], Array[Byte]](
      topicName,
      key.getBytes(StandardCharsets.UTF_8),
      value.toString().getBytes(StandardCharsets.UTF_8)
    )

    cache.get(kafkaConfig).send(record)
  }

  private def load() = new CacheLoader[TelemetryMetadataProducerConfig, KafkaProducer[Array[Byte], Array[Byte]]] {

    override def load(config: TelemetryMetadataProducerConfig): KafkaProducer[Array[Byte], Array[Byte]] = {

      val kafkaConfig = config.global

      val telemetryConfig = config.telemetry

      val connectionString = kafkaConfig.connections
        .map { conn =>
          s"${conn.host}:${conn.port}"
        }
        .mkString(",")

      val props = new Properties()
      props.put("bootstrap.servers", connectionString)
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

      val notOverridableKeys = props.keySet.asScala

      val merged: Seq[KafkaEntryConfig] = kafkaConfig.others ++ telemetryConfig.telemetryTopicConfigModel.kafkaSettings

      val resultingConf = merged.filterNot(x => notOverridableKeys.contains(x.key))

      resultingConf.foreach {
        case KafkaEntryConfig(key, value) => props.put(key, value)
      }

      new KafkaProducer[Array[Byte], Array[Byte]](props)

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

      stream
        .withColumn(
          "metadata_new",
          updateFunction(
            col("metadata.id"),
            col("metadata.sourceId"),
            col("metadata.arrivalTimestamp"),
            col("metadata.lastSeenTimestamp"),
            col("metadata.path")
          )
        )
        .drop("metadata")
        .withColumnRenamed("metadata_new", "metadata")
        .select(originalColumnsOrder.head, originalColumnsOrder.tail: _*)
    } else {
      stream
    }

  def updateMetadata(path: String): UserDefinedFunction = udf {
    (mId: String, mSourceId: String, mArrivalTimestamp: Long, _: Long, mPath: Seq[Row]) =>
      {

        val nowInstant = Clock.systemUTC().instant()
        val now        = nowInstant.toEpochMilli
        val oldPaths   = mPath.map(r => Path(r))
        val newPaths   = (oldPaths :+ Path(path, now)).toArray

        Metadata(mId, mSourceId, mArrivalTimestamp, now, newPaths)

      }
  }

  def sendLatencyMessage(stream: DataFrame, config: TelemetryMetadataProducerConfig, samplingFactor: Int): DataFrame =
    if (stream.columns.contains("metadata")) {

      implicit val rowEncoder: Encoder[Row] = RowEncoder(stream.schema)

      stream.mapPartitions { partition: Iterator[Row] =>
        var counter = 0

        partition.map { row =>
          if (counter % samplingFactor == 0) {

            val metadata = row.getStruct(row.fieldIndex("metadata"))

            val pathField = metadata.fieldIndex("path")

            val messageId = metadata.getString(metadata.fieldIndex("id"))

            val sourceId = metadata.getString(metadata.fieldIndex("sourceId"))

            val arrivalTimestamp = metadata.getLong(metadata.fieldIndex("arrivalTimestamp"))

            val path = Path(sourceId, arrivalTimestamp) +: metadata.getSeq[Row](pathField).map(Path.apply)

            val lastTwoHops = path.takeRight(2)

            val latency = lastTwoHops(1).ts - lastTwoHops(0).ts

            val collectionTimeAsString = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(lastTwoHops(1).ts))

            val compositeSourceId = path.map(_.name.replace(' ', '-')).mkString("/")

            val message =
              MetricsTelemetryMessage(messageId, compositeSourceId, "latencyMs", latency, collectionTimeAsString)

            val json = MetricsTelemetryMessageFormat.metricsTelemetryMessageFormat.write(message)

            TelemetryMetadataProducer.send(config, messageId, json)

          }

          counter = counter + 1
          row
        }

      }
    } else {
      stream
    }

}
