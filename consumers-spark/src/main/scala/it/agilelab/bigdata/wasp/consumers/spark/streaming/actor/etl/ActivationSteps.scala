package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import java.util.UUID

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.metadata.{Metadata, Path}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.MetadataUtils
import it.agilelab.bigdata.wasp.core.bl.{MlModelBL, TopicBL}
import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
  protected   val topicsBl: TopicBL


  /**
    * Performs activation of a [[StructuredStreamingETLModel]] returning the output data frame
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
    * @param etl The etl to activate streaming sources for
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
    * @param etl The etl whose strategy should be applied
    * @param structuredInputStream The input stream from kafka
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
    * @param readerKey The key to place the resulting stream in the map passed to the strategy
    * @param stream The input stream coming from kafka
    * @param dataStoreDFs The data frames representing non streaming data stores
    * @param strategy The strategy to be applied
    * @param writerType The type of the output writer, will be used to properly handle metadata schema
    * @param etl The etl model
    * @return A Try representing the application of the strategy as a new DataFrame
    */
  private def applyTransform(readerKey: ReaderKey,
                             stream: DataFrame,
                             dataStoreDFs: Map[ReaderKey, DataFrame],
                             strategy: Strategy,
                             writerType: WriterType,
                             etl: StructuredStreamingETLModel): Try[DataFrame] = Try {

    val etlName = etl.name


    val now = System.currentTimeMillis()

    val updateMetadata = udf(
      (mId: String,
       mSourceId: String,
       mArrivalTimestamp: Long,
       mLastSeenTimestamp: Long,
       mPath: Seq[Row]) => {

        if (mId == "") {
          Metadata(UUID.randomUUID().toString,
            mSourceId,
            now,
            now,
            Seq(Path(etlName, now)).toArray)
        }
        else {
          val oldPaths = mPath.map(r => Path(r))
          Metadata(mId,
            mSourceId,
            mArrivalTimestamp,
            now,
            (oldPaths :+ Path(etlName, now)).toArray)
        }
      })

    //update values in field metadata
    val dataframeToTransform = if (stream.columns.contains("metadata")) {
      stream
        .withColumn(
          "metadata_new",
          updateMetadata(col("metadata.id"),
            col("metadata.sourceId"),
            col("metadata.arrivalTimestamp"),
            col("metadata.lastSeenTimestamp"),
            col("metadata.path"))
        )
        .drop("metadata")
        .withColumnRenamed("metadata_new", "metadata")
    }
    else {
      stream
    }

    val completeMapOfDFs: Map[ReaderKey, DataFrame] = dataStoreDFs + (readerKey -> dataframeToTransform)

    val output = strategy.transform(completeMapOfDFs)

    writerType.getActualProduct match {
      case Datastores.kafkaProduct => output
      case Datastores.hbaseProduct => output
      case Datastores.rawProduct => output
      case Datastores.consoleProduct => output
      case _ =>
        if (output.columns.contains("metadata")) {
          output.select(MetadataUtils.flatMetadataSchema(output.schema, None): _*)
        }
        else
          output
    }
  }


}
