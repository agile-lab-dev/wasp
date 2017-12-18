package it.agilelab.bigdata.wasp.consumers.spark

import java.util.UUID

import akka.actor.{Actor, ActorRef, actorRef2Scala}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{
  MlModelsBroadcastDB,
  MlModelsDB
}
import it.agilelab.bigdata.wasp.consumers.spark.metadata.{Metadata, Path}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{
  SparkReader,
  StreamingReader
}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.MetadataUtils
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class LegacyStreamingETLActor(env: {
                                val topicBL: TopicBL
                                val indexBL: IndexBL
                                val rawBL: RawBL
                                val keyValueBL: KeyValueBL
                                val mlModelBL: MlModelBL
                              },
                              sparkWriterFactory: SparkWriterFactory,
                              streamingReader: StreamingReader,
                              ssc: StreamingContext,
                              etl: LegacyStreamingETLModel,
                              listener: ActorRef,
                              plugins: Map[String, WaspConsumersSparkPlugin])
    extends Actor
    with Logging {

  case object StreamReady

  /*
   * Actor methods start
   */

  def receive: Actor.Receive = {
    case StreamReady => listener ! OutputStreamInitialized
  }

  // TODO check if mainTask has really to be invoked here
  override def preStart(): Unit = {
    super.preStart()
    logger.info(s"Actor is transitioning from 'uninitialized' to 'initialized'")
    validationTask()
    mainTask()
  }

  /*
   * Actor methods end
   */

  /**
    * Strategy object initialization
    */
  private lazy val createStrategy: Option[Strategy] = etl.strategy match {
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
    * All readers initialization
    */
  private def allReaders(readers: List[ReaderModel]): List[SparkReader] = {
    readers.flatMap({
      case ReaderModel(name, endpointId, readerType) =>
        val readerProduct = readerType.getActualProduct
        logger.info(s"Get reader plugin $readerProduct before was $readerType, plugin map: $plugins")
        val readerPlugin = plugins.get(readerProduct)
        if (readerPlugin.isDefined) {
          Some(readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name))
        } else {
          logger.error(s"The $readerProduct plugin in allReaders does not exists")
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
  private def staticReaders(readers: List[ReaderModel]): List[SparkReader] = allReaders(readers)

  /**
    * Topic models initialization
    */
  private def topicModels(): List[Option[TopicModel]] =
    etl.inputs
      .flatMap({
        case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) =>
          val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
          Some(topicOpt)
        case _ => None
      })

  private def validationTask(): Unit = {
    etl.inputs.foreach({
      case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) => {
        val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
        if (topicOpt.isEmpty) {
          //TODO Better exception
          throw new Exception(s"There isn't this topic: $endpointId, $name")
        }
      }
      case ReaderModel(name, endpointId, readerType) => {
        val readerPlugin = plugins.get(readerType.getActualProduct)
        if (readerPlugin.isDefined) {
          readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name)
        } else {
          //TODO Better exception
          logger.error(
            s"There isn't the plugin for this index: '$endpointId', '$name', readerType: '$readerType'")
          throw new Exception(s"There isn't this index: $endpointId, $name")
        }
      }
    })
    val topicReaderModelNumber =
      etl.inputs.count(_.readerType.category == TopicModel.readerType)
    if (topicReaderModelNumber == 0)
      throw new Exception("There is NO topic to read data, inputs: " + etl.inputs)
    if (topicReaderModelNumber != 1)
      throw new Exception("MUST be only ONE topic, inputs: " + etl.inputs)
  }

  //TODO move in the extender class
  def mainTask(): Unit = {

    val topicStreams: List[(ReaderKey, DStream[String])] =
      topicModels().map(topicModelOpt => {
        assert(topicModelOpt.isDefined)

        val topicModel = topicModelOpt.get
        val stream: DStream[String] =
          streamingReader.createStream(etl.group,
                                       etl.kafkaAccessType,
                                       topicModel)(ssc = ssc)
        (ReaderKey(TopicModel.readerType, topicModel.name), stream)
      })

    //TODO  Join condition between streams

    assert(topicStreams.nonEmpty)
    assert(topicStreams.size == 1)

    val topicStreamWithKey: (ReaderKey, DStream[String]) = topicStreams.head

    val outputStream =
      if (createStrategy.isDefined) {
        val strategy = createStrategy.get

        val readers = etl.inputs

        val dataStoreDFs : Map[ReaderKey, DataFrame] =

          // print a warning when no readers are defined
          if(readers.isEmpty) {
            logger.warn("Readers list empty!")
            Map.empty
          }
          else
            retrieveDFs(readers)

        // TODO check if require abort processing (see BatchJobActor) - NB kafka reader will not be found as plugin, so in dataStoreDFs
        val nDFrequired = readers.size
        val nDFretrieved = dataStoreDFs.size
        if(nDFretrieved != nDFrequired) {
          // abort processing
          logger.error("DFs not retrieved successfully!")
          logger.error(s"$nDFrequired DFs required - $nDFretrieved DFs retrieved!")
          logger.error(dataStoreDFs.toString)
          //changeBatchState(jobModel._id.get, JobStateEnum.FAILED)
        }
        //else {
        //  if(!dataStoreDFs.isEmpty)
        //    logger.info("DFs retrieved successfully!")

        val mlModelsDB = new MlModelsDB(env)
        // --- Broadcast models initialization ----
        // Reading all model from DB and create broadcast
        val mlModelsBroadcast: MlModelsBroadcastDB =
          mlModelsDB.createModelsBroadcast(etl.mlModels)(ssc.sparkContext)

        // Initialize the mlModelsBroadcast to strategy object
        strategy.mlModelsBroadcast = mlModelsBroadcast

        transform(topicStreamWithKey._1,
                  topicStreamWithKey._2,
                  dataStoreDFs,
                  strategy,
                  etl.output.writerType)
      } else {
        topicStreamWithKey._2
      }

    val sparkWriterOpt =
      sparkWriterFactory.createSparkWriterStreaming(env, ssc, etl.output)

    sparkWriterOpt match {

      case Some(writer) =>
        writer.write(outputStream)

      case None =>
        val error =
          s"No Spark Streaming writer available for writer ${etl.output}"
        logger.error(error)
        throw new Exception(error)

    }

    // For some reason, trying to send directly a message from here to the guardian is not working ...
    // NOTE: Maybe because mainTask is invoked in preStart ?
    // TODO check required
    logger.info(s"Actor is notifying the guardian that it's ready")
    self ! StreamReady
  }

  private def retrieveDFs(readerModels: List[ReaderModel]) : Map[ReaderKey, DataFrame] = {
    // Reading static source to DF
    staticReaders(readerModels)
      .flatMap(staticReader => {
        try {
          val dataSourceDF = staticReader.read(ssc.sparkContext)
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
                        stream: DStream[String],
                        dataStoreDFs: Map[ReaderKey, DataFrame],
                        strategy: Strategy,
                        writerType: WriterType): DStream[String] = {
    val sqlContext = SparkSingletons.getSQLContext
    val strategyBroadcast = ssc.sparkContext.broadcast(strategy)

    val etlName = etl.name

    stream.transform(rdd => {

      //TODO Verificare se questo è il comportamento che si vuole

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

        val completeMapOfDFs
          : Map[ReaderKey, DataFrame] = dataStoreDFs + (readerKey -> dataframeToTransform)

        val output = strategyBroadcast.value.transform(completeMapOfDFs)

        writerType.getActualProduct match {
          case Datastores.kafkaProduct => output.toJSON.rdd
          case Datastores.hbaseProduct => output.toJSON.rdd
          case Datastores.rawProduct => output.toJSON.rdd
          case Datastores.consoleProduct => output.toJSON.rdd
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
