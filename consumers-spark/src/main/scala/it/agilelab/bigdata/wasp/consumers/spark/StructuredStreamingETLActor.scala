package it.agilelab.bigdata.wasp.consumers.spark

import java.util.UUID

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.metadata.{Metadata, Path}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.MetadataUtils
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils._
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian.generateUniqueComponentName
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.StopProcessingComponent
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.SparkStreamingConfiguration
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class StructuredStreamingETLActor(env: {
                                    val topicBL: TopicBL
                                    val indexBL: IndexBL
                                    val rawBL: RawBL
                                    val keyValueBL: KeyValueBL
                                    val mlModelBL: MlModelBL
                                  },
                                  sparkWriterFactory: SparkWriterFactory,
                                  structuredStreamingReader: StructuredStreamingReader,
                                  sparkSession: SparkSession,
                                  pipegraph: PipegraphModel,
                                  structuredStreamingETL: StructuredStreamingETLModel,
                                  listener: ActorRef,
                                  plugins: Map[String, WaspConsumersSparkPlugin])
    extends Actor
    with SparkStreamingConfiguration
    with Logging {

  /*
   * Actor methods start
   */

  override def receive: Actor.Receive = {
    case StopProcessingComponent => {
      logger.info(s"Component actor $self stopping...")
      stopProcessingComponent()
      context stop self
      logger.info(s"Component actor $self stopped")
    }
  }

  override def preStart(): Unit = {
    logger.info(s"Actor is transitioning from 'uninitialized' to 'initialized'")
    try {
      validationTask()
      mainTask()
    } catch {
      case e: Exception =>
        val msg = s"Pipegraph '${pipegraph.name}' - StructuredStreamingETLActor '${structuredStreamingETL.name}': Exception: ${e.getMessage}"
        logger.error(msg)
        listener ! Left(msg)
        
      case e: Error =>
        val msg = s"Pipegraph '${pipegraph.name}' - StructuredStreamingETLActor '${structuredStreamingETL.name}': Error: ${e.getMessage}"
        logger.error(msg)
        listener ! Left(msg)
    }
  }

  /*
   * Actor methods end
   */

  /**
    * Strategy object initialization
    */
  private lazy val createStrategy: Option[Strategy] = structuredStreamingETL.strategy match {
    case None => None
    case Some(strategyModel) =>
      val result = Class
        .forName(strategyModel.className)
        .newInstance()
        .asInstanceOf[Strategy]
      result.configuration = strategyModel.configurationConfig() match {
        case None                => ConfigFactory.empty()
        case Some(configuration) => configuration
      }

      logger.info("strategy: " + result)
      Some(result)
  }

  /**
    * All static readers initialization
    */
  private def allStaticReaders(staticReaderModels: List[ReaderModel]): List[SparkReader] = {
    staticReaderModels.flatMap({
      case ReaderModel(name, endpointId, readerType) =>
        val readerProduct = readerType.getActualProduct
        logger.info(s"Get reader plugin $readerProduct before was $readerType, plugin map: $plugins")
        val readerPlugin = plugins.get(readerProduct)
        if (readerPlugin.isDefined) {
          Some(readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name))
        } else {
          logger.error(s"The $readerProduct plugin in staticReaderModels does not exists")
          None
        }
      case _ => None
    })
  }

  /**
    * All static readers initialization
    *
    * @return
    */
  private def retrieveStaticReaders(staticReaderModels: List[ReaderModel]): List[SparkReader] = allStaticReaders(staticReaderModels)

  /**
    * Topic models initialization
    */
  private def topicModels(): List[Option[TopicModel]] =
    structuredStreamingETL.inputs
      .flatMap({
        case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) =>
          val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
          Some(topicOpt)
        case _ => None
      })

  private def validationTask(): Unit = {
    structuredStreamingETL.inputs.foreach({
      case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) => {
        val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
        if (topicOpt.isEmpty) {
          throw new Exception(s"There isn't this topic: $endpointId, $name")
        }
      }
      case ReaderModel(name, endpointId, readerType) => {
        val readerPlugin = plugins.get(readerType.getActualProduct)
        if (readerPlugin.isDefined) {
          readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name)
        } else {
          throw new Exception(s"There isn't the plugin for this index: '$endpointId', '$name', readerType: '$readerType'")
        }
      }
    })
    val topicReaderModelNumber =
      structuredStreamingETL.inputs.count(_.readerType.category == TopicModel.readerType)
    if (topicReaderModelNumber == 0)
      throw new Exception("There is NO topic to read data, inputs: " + structuredStreamingETL.inputs)
    if (topicReaderModelNumber != 1)
      throw new Exception("MUST be only ONE topic, inputs: " + structuredStreamingETL.inputs)
  }

  //TODO move in the extender class
  def mainTask(): Unit = {

    val topicStreams: List[(ReaderKey, DataFrame)] =
      topicModels().map(topicModelOpt => {
        assert(topicModelOpt.isDefined)

        val topicModel = topicModelOpt.get
        val stream: DataFrame =
          structuredStreamingReader.createStructuredStream(
            structuredStreamingETL.group,
            structuredStreamingETL.kafkaAccessType,
            topicModel)(sparkSession)
        (ReaderKey(TopicModel.readerType, topicModel.name), stream)
      })

    //TODO  Join condition between streams

    assert(topicStreams.nonEmpty)
    assert(topicStreams.size == 1)

    val topicStreamWithKey: (ReaderKey, DataFrame) = topicStreams.head

    val outputStream =
      if (createStrategy.isDefined) {
        val strategy = createStrategy.get

        val staticReaders = structuredStreamingETL.inputs.filterNot(_.readerType.category == Datastores.topicCategory)

        val dataStoreDFs : Map[ReaderKey, DataFrame] =
          if(staticReaders.isEmpty)
            Map.empty
          else
            retrieveDFs(staticReaders)

        val nDFrequired = staticReaders.size
        val nDFretrieved = dataStoreDFs.size
        if(nDFretrieved != nDFrequired) {
          val error = "DFs not retrieved successfully!\n" +
            s"$nDFrequired DFs required - $nDFretrieved DFs retrieved!\n" +
            dataStoreDFs.toString
          logger.error(error) // print here the complete error due to verbosity

          throw new Exception(s"DFs not retrieved successful - $nDFrequired DFs required - $nDFretrieved DFs retrieved!")
        }
        else {
          if (!dataStoreDFs.isEmpty)
            logger.info("DFs retrieved successfully!")
        }

        val mlModelsDB = new MlModelsDB(env)
        // --- Broadcast models initialization ----
        // Reading all model from DB and create broadcast
        val mlModelsBroadcast: MlModelsBroadcastDB =
        mlModelsDB.createModelsBroadcast(structuredStreamingETL.mlModels)(sparkSession.sparkContext)

        // Initialize the mlModelsBroadcast to strategy object
        strategy.mlModelsBroadcast = mlModelsBroadcast

        transform(topicStreamWithKey._1,
          topicStreamWithKey._2,
          dataStoreDFs,
          strategy,
          structuredStreamingETL.output.writerType)
      } else {
        topicStreamWithKey._2
      }

    val queryName = generateUniqueComponentName(pipegraph, structuredStreamingETL)
    val checkpointDir = generateStructuredStreamingCheckpointDir(
      sparkStreamingConfig,
      pipegraph,
      structuredStreamingETL)

    val sparkWriterOpt = sparkWriterFactory.createSparkWriterStructuredStreaming(env, sparkSession, structuredStreamingETL.output)
    sparkWriterOpt match {
      case Some(writer) => writer.write(outputStream, queryName, checkpointDir)
      case None => throw new Exception(s"No Spark Structured Streaming writer available for writer ${structuredStreamingETL.output}")
    }

    val msg = s"Pipegraph '${pipegraph.name}' - StructuredStreamingETLActor '${structuredStreamingETL.name}' started"
    listener ! Right(msg)
  }

  private def retrieveDFs(staticReaderModels: List[ReaderModel]) : Map[ReaderKey, DataFrame] = {
    // Reading static source to DF
    retrieveStaticReaders(staticReaderModels)
      .flatMap(staticReader => {
        try {
          val dataSourceDF = staticReader.read(sparkSession.sparkContext)
          Some(ReaderKey(staticReader.readerType, staticReader.name), dataSourceDF)
        } catch {
          case e: Exception => {
            logger.error(s"Error during retrieving DF: ${staticReader.name}", e)
            None
          }
        }
      })
      .toMap
  }

  private def transform(readerKey: ReaderKey,
                        stream: DataFrame,
                        dataStoreDFs: Map[ReaderKey, DataFrame],
                        strategy: Strategy,
                        writerType: WriterType): DataFrame = {

    val strategyBroadcast = sparkSession.sparkContext.broadcast(strategy)

    val etlName = structuredStreamingETL.name

    logger.debug(s"input stream: ${readerKey.name}. struct: ${stream.schema.treeString}")

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
    val dataframeToTransform = if(stream.columns.contains("metadata")) {
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

    val output = strategyBroadcast.value.transform(completeMapOfDFs)

    writerType.getActualProduct match {
      case Datastores.kafkaProduct => output
      case Datastores.hbaseProduct => output
      case Datastores.rawProduct => output
      case Datastores.consoleProduct => output
      case _ =>
        if(output.columns.contains("metadata")) {
          logger.info(s"Metadata to be flattened for writer category ${writerType.category}. original output schema: ${output.schema.treeString}")
          output.select(MetadataUtils.flatMetadataSchema(output.schema, None): _*)
        }
        else
          output
    }
  }

  /**
    * Stops the processing component belonging to this component actor
    */
  private def stopProcessingComponent(): Unit = {
    val queryName = generateUniqueComponentName(pipegraph, structuredStreamingETL)
    logger.info(s"Stopping component $queryName")

    val structuredQueryOpt = sparkSession.streams.active.find(_.name == queryName)
    structuredQueryOpt match {
      case Some(structuredQuery) =>
        logger.info(s"Found StructuredQuery $structuredQuery corresponding to component $queryName, stopping it...")
        structuredQuery.stop()        // TODO: may throws an exception to handle - see ISC-318
        logger.info(s"Successfully stopped StructuredQuery corresponding to component $queryName")

      case None =>
        logger.warn(s"No matching StructuredQuery found for component $queryName! Maybe it has already been stopped?")
    }
  }
}