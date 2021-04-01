package it.agilelab.bigdata.wasp.consumers.spark.streaming

import java.util.UUID

import akka.actor.{Actor, ActorRef, PoisonPill, actorRef2Scala}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.metadata.{Metadata, Path}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{SparkBatchReader, SparkLegacyStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.MetadataUtils
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.repository.core.bl._
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct._
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{LegacyStreamingETLModel, PipegraphModel, ReaderModel, TopicModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class LegacyStreamingETLActor(env: {
                                val topicBL: TopicBL
                                val indexBL: IndexBL
                                val rawBL: RawBL
                                val keyValueBL: KeyValueBL
                                val mlModelBL: MlModelBL
                              },
                              sparkWriterFactory: SparkWriterFactory,
                              streamingReader: SparkLegacyStreamingReader,
                              ssc: StreamingContext,
                              pipegraph: PipegraphModel,
                              legacyStreamingETL: LegacyStreamingETLModel,
                              listener: ActorRef,
                              plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin])
  extends Actor
    with Logging {

  /*
   * Actor methods start
   */

  override def receive: Actor.Receive = {
    case PoisonPill => context stop self  // workaround to allow to not manage none Message (not required for LegacyStreamingETLActor)
  }

  override def preStart(): Unit = {
    logger.info(s"Actor is transitioning from 'uninitialized' to 'initialized'")
    try {
      validationTask()
      mainTask()
    } catch {
      case e: Exception =>
        val msg = s"Pipegraph '${pipegraph.name}' - LegacyStreamingETLActor '${legacyStreamingETL.name}': Exception: ${e.getMessage}"
        logger.error(msg, e)
        listener ! Left(msg)

      case e: Error =>
        val msg = s"Pipegraph '${pipegraph.name}' - LegacyStreamingETLActor '${legacyStreamingETL.name}': Error: ${e.getMessage}"
        logger.error(msg, e)
        listener ! Left(msg)
    }
  }

  /*
   * Actor methods end
   */

  /**
    * Strategy object initialization
    */
  private lazy val createStrategy: Option[Strategy] = legacyStreamingETL.strategy match {
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
  private def allStaticReaders(staticReaderModels: List[ReaderModel]): List[SparkBatchReader] = {
    staticReaderModels.flatMap({
      case readerModel =>
        val datastoreProduct = readerModel.datastoreProduct
        logger.info(s"Finding reader plugin for datastore product $datastoreProduct")
        val readerPlugin = plugins.get(datastoreProduct)
        if (readerPlugin.isDefined) {
          Some(readerPlugin.get.getSparkBatchReader(ssc.sparkContext, readerModel))
        } else {
          logger.error(s"No plugin found for datastore product $datastoreProduct!")
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
  private def retrieveStaticReaders(staticReaderModels: List[ReaderModel]): List[SparkBatchReader] = allStaticReaders(staticReaderModels)

  /**
    * Topic models initialization
    */
  private def topicModels(): List[Option[TopicModel]] =
    legacyStreamingETL.inputs
      .flatMap({
        case ReaderModel(name, datastoreModelName, KafkaProduct, options) =>
          val topicOpt = env.topicBL.getTopicModelByName(datastoreModelName)
          Some(topicOpt)
        case _ => None
      })

  private def validationTask(): Unit = {
    // WTF is the point of this? for topics we check the topic model, for indexes we check the plugin, for the other ones... nothing?
    /*
    legacyStreamingETL.inputs.foreach({
      case ReaderModel(name, datastoreModelName, KafkaProduct, options) => {
        val topicOpt = env.topicBL.getByName(datastoreModelName)
        if (topicOpt.isEmpty) {
          throw new Exception(s"There isn't this topic: $datastoreModelName, $name")
        }
      }
      case ReaderModel(name, datastoreModelName, indexProduct, options) if indexProduct.isInstanceOf[IndexCategory] => {
        val readerPlugin = plugins.get(indexProduct)
        if (readerPlugin.isDefined) {
          readerPlugin.get.getSparkBatchReader(datastoreModelName, name)
        } else {
          throw new Exception(s"There isn't the plugin for this index: '$datastoreModelName', '$name', readerType: '$indexProduct'")
        }
      }
    })
    */
    val topicReaderModelNumber =
      legacyStreamingETL.inputs.count(_.datastoreProduct.categoryName == GenericTopicProduct.categoryName)
    if (topicReaderModelNumber == 0)
      throw new Exception("There is NO topic to read data, inputs: " + legacyStreamingETL.inputs)
    if (topicReaderModelNumber != 1)
      throw new Exception("MUST be only ONE topic, inputs: " + legacyStreamingETL.inputs)
  }

  def mainTask(): Unit = {

    val topicStreams: List[(ReaderKey, DStream[String])] =
      topicModels().map(topicModelOpt => {
        assert(topicModelOpt.isDefined)

        val topicModel = topicModelOpt.get
        val stream: DStream[String] =
          streamingReader.createStream(
            legacyStreamingETL.group,
            legacyStreamingETL.kafkaAccessType,
            topicModel)(ssc)
        (ReaderKey(GenericTopicProduct.categoryName, topicModel.name), stream)
      })

    //TODO  Join condition between streams

    assert(topicStreams.nonEmpty)
    assert(topicStreams.size == 1)

    val topicStreamWithKey: (ReaderKey, DStream[String]) = topicStreams.head

    val outputStream =
      if (createStrategy.isDefined) {
        val strategy = createStrategy.get

        val staticReaders = legacyStreamingETL.inputs.filterNot(_.datastoreProduct.categoryName == GenericTopicProduct.categoryName)

        val dataStoreDFs : Map[ReaderKey, DataFrame] =
          if (staticReaders.isEmpty)
            Map.empty
          else
            retrieveDFs(staticReaders)

        val nDFrequired = staticReaders.size
        val nDFretrieved = dataStoreDFs.size
        if (nDFretrieved != nDFrequired) {
          val msg = "DFs not retrieved successfully!\n" +
            s"$nDFrequired DFs required - $nDFretrieved DFs retrieved!\n" +
            dataStoreDFs.toString
          logger.error(msg) // print here the complete error due to verbosity

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
        mlModelsDB.createModelsBroadcast(legacyStreamingETL.mlModels)(ssc.sparkContext)

        // Initialize the mlModelsBroadcast to strategy object
        strategy.mlModelsBroadcast = mlModelsBroadcast

        transform(topicStreamWithKey._1,
          topicStreamWithKey._2,
          dataStoreDFs,
          strategy,
          legacyStreamingETL.output.datastoreProduct)
      } else {
        topicStreamWithKey._2
      }

    val sparkWriterOpt = sparkWriterFactory.createSparkWriterLegacyStreaming(env, ssc, legacyStreamingETL, legacyStreamingETL.output)
    sparkWriterOpt match {
      case Some(writer) => writer.write(outputStream)
      case None => throw new Exception(s"No Spark Streaming writer available for writer ${legacyStreamingETL.output}")
    }

    val msg = s"Pipegraph '${pipegraph.name}' - LegacyStreamingETLActor '${legacyStreamingETL.name}' started"
    listener ! Right(msg)
  }

  private def retrieveDFs(staticReaderModels: List[ReaderModel]) : Map[ReaderKey, DataFrame] = {
    // Reading static source to DF
    retrieveStaticReaders(staticReaderModels)
      .flatMap(staticReader => {
        try {
          val dataSourceDF = staticReader.read(ssc.sparkContext)
          Some(ReaderKey(staticReader.readerType, staticReader.name), dataSourceDF)
        } catch {
          case e: Exception =>
            logger.error(s"Error during retrieving DF: ${staticReader.name}", e)
            None
        }
      })
      .toMap
  }

  private def transform(readerKey: ReaderKey,
                        stream: DStream[String],
                        dataStoreDFs: Map[ReaderKey, DataFrame],
                        strategy: Strategy,
                        datastoreProduct: DatastoreProduct): DStream[String] = {

    val sqlContext = SparkSingletons.getSQLContext

    /** broadcast Strategy required when Strategy is not serializable, e.g. extends Logging (java.io.NotSerializableException: it.agilelab.bigdata.wasp.core.logging.WaspLogger) */
    val strategyBroadcast = ssc.sparkContext.broadcast(strategy)

    val etlName = legacyStreamingETL.name

    logger.debug(s"input stream: ${readerKey.name}")

    stream.transform(rdd => {
      val df = sqlContext.read.json(rdd)
      if (df.schema.nonEmpty) {

        val updateMetadata = udf(
          (mId: String,
           mSourceId: String,
           mArrivalTimestamp: Long,
           mLastSeenTimestamp: Long,
           mPath: Seq[Path]) => {

            val now = System.currentTimeMillis()

            if (mId == "")
              Metadata(UUID.randomUUID().toString,
                mSourceId,
                now,
                now,
                Seq(Path(etlName, now)).toArray)
            else
              Metadata(mId,
                mSourceId,
                mArrivalTimestamp,
                now,
                (mPath :+ Path(etlName, now)).toArray)
          })

        // TODO check if metadata exists
        // TODO improve flatMetadataSchema functions

        //update values in field metadata
        var dataframeToTransform = df
          .withColumn(
            "metadata",
            updateMetadata(col("metadata.id"),
              col("metadata.sourceId"),
              col("metadata.arrivalTimestamp"),
              col("metadata.lastSeenTimestamp"),
              col("metadata.path"))
          )

        val completeMapOfDFs: Map[ReaderKey, DataFrame] = dataStoreDFs + (readerKey -> dataframeToTransform)

        /** use of the broadcasted Strategy required when Strategy is not serializable, e.g. extends Logging (java.io.NotSerializableException: it.agilelab.bigdata.wasp.core.logging.WaspLogger) */
        val output = strategyBroadcast.value.transform(completeMapOfDFs)

        datastoreProduct match {
          case KafkaProduct => output.toJSON.rdd
          case HBaseProduct => output.toJSON.rdd
          case RawProduct => output.toJSON.rdd
          case ConsoleProduct => output.toJSON.rdd
          case _ => {
            output
              .select(MetadataUtils.flatMetadataSchema(df.schema, None): _*)
              .toJSON
              .rdd
          }
        }

      } else {
        rdd
      }
    })
  }
}