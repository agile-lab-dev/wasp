package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumerSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.{StaticReader, StructuredStreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{ETLStructuredModel, ReaderModel, ReaderType, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class ConsumerETLStructuredActor(env: {
  val topicBL: TopicBL;
  val indexBL: IndexBL;
  val rawBL: RawBL;
  val keyValueBL: KeyValueBL;
  val mlModelBL: MlModelBL
}, sparkWriterFactory: SparkWriterFactory, structuredStreamingReader: StructuredStreamingReader, ss: SparkSession, etlStructured: ETLStructuredModel, listener: ActorRef, plugins: Map[String, WaspConsumerSparkPlugin])
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
    * Strategy object initialize
    */
  //TODO: identical to it.agilelab.bigdata.wasp.consumers.spark.batch.BatchJobActor.createStrategy, externalize
  private lazy val createStrategy: Option[Strategy] =
    etlStructured.strategy match {
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
    * Index readers initialization
    */
  private def indexReaders(): List[StaticReader] = {
    val defaultDataStoreIndexed =
      ConfigManager.getWaspConfig.defaultIndexedDatastore
    etlStructured.inputs.flatMap({
      case ReaderModel(name, endpointId, readerType) =>
        val readerProduct = readerType.getActualProduct
        logger.info(
          s"Get index reader plugin $readerProduct before was $readerType, plugin map: $plugins")

        val readerPlugin = plugins.get(readerProduct)
        if (readerPlugin.isDefined) {
          Some(
            readerPlugin.get.getSparkReader(endpointId.getValue.toHexString,
                                            name))
        } else {
          //TODO Check if readerType != topic
          logger.warn(
            s"The $readerProduct plugin in indexReaders does not exists")
          None
        }
    })
  }

  /**
    * Raw readers initialization
    */
  private def rawReaders(): List[StaticReader] =
    etlStructured.inputs
      .flatMap({
        case ReaderModel(name, endpointId, readerType) =>
          logger.info(
            s"Get raw reader plugin $readerType, plugin map: $plugins")
          val readerPlugin = plugins.get(readerType.getActualProduct)
          if (readerPlugin.isDefined) {
            Some(
              readerPlugin.get.getSparkReader(endpointId.getValue.toHexString,
                                              name))
          } else {
            //TODO Check if readerType != topic
            logger.error(
              s"The $readerType plugin in rawReaders does not exists")
            None
          }
      })

  // TODO unify readers initialization (see BatchJobActor)
  /**
    * All static readers initialization
    *
    * @return
    */
  private def staticReaders(): List[StaticReader] =
    indexReaders() ++ rawReaders()

  /**
    * Topic models initialization
    */
  private def topicModels(): List[Option[TopicModel]] =
    etlStructured.inputs
      .flatMap({
        case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) =>
          val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
          Some(topicOpt)
        case _ => None
      })

  private def validationTask(): Unit = {
    etlStructured.inputs.foreach({
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
      etlStructured.inputs.count(_.readerType.category == TopicModel.readerType)
    if (topicReaderModelNumber == 0)
      throw new Exception(
        "There is NO topic to read data, inputs: " + etlStructured.inputs)
    if (topicReaderModelNumber != 1)
      throw new Exception(
        "MUST be only ONE topic, inputs: " + etlStructured.inputs)
  }

  //TODO move in the extender class
  def mainTask(): Unit = {

    val topicStreams: List[(ReaderKey, DataFrame)] =
      topicModels().map(topicModelOpt => {
        assert(topicModelOpt.isDefined)

        val topicModel = topicModelOpt.get
        val stream: DataFrame =
          structuredStreamingReader.createStructuredStream(etlStructured.group,
                                                 etlStructured.kafkaAccessType,
                                                 topicModel)(ss)
        (ReaderKey(TopicModel.readerType, topicModel.name), stream)
      })

    //TODO  Join condition between streams

    assert(topicStreams.nonEmpty)
    assert(topicStreams.size == 1)

    val topicStreamWithKey: (ReaderKey, DataFrame) = topicStreams.head

    val outputStream: DataFrame =
      if (createStrategy.isDefined) {
        val strategy = createStrategy.get

        //TODO cache or reading?
        // Reading static source to DF
        val dataStoreDFs: Map[ReaderKey, DataFrame] =
          staticReaders()
            .map(staticReader => {

              val dataSourceDF = staticReader.read(ss.sparkContext)
              (ReaderKey(staticReader.readerType, staticReader.name),
               dataSourceDF)
            })
            .toMap

        val mlModelsDB = new MlModelsDB(env)
        // --- Broadcast models initialization ----
        // Reading all model from DB and create broadcast
        val mlModelsBroadcast: MlModelsBroadcastDB =
          mlModelsDB.createModelsBroadcast(etlStructured.mlModels)(ss.sparkContext)

        // Initialize the mlModelsBroadcast to strategy object
        strategy.mlModelsBroadcast = mlModelsBroadcast
        transform(topicStreamWithKey._1,
                  topicStreamWithKey._2,
                  dataStoreDFs,
                  strategy)
      } else {
        topicStreamWithKey._2
      }

    val sparkWriterOpt =
      sparkWriterFactory.createSparkWriterStructuredStreaming(env, ss, etlStructured.output)
    sparkWriterOpt.foreach(w => {
      w.write(outputStream)
    })

    // For some reason, trying to send directly a message from here to the guardian is not working ...
    // NOTE: Maybe because mainTask is invoked in preStart ?
    // TODO check required
    logger.info(s"Actor is notifying the guardian that it's ready")
    self ! StreamReady
  }

  private def transform(readerKey: ReaderKey,
                        stream: DataFrame,
                        dataStoreDFs: Map[ReaderKey, DataFrame],
                        strategy: Strategy): DataFrame = {
    val sqlContext = SQLContextSingleton.getInstance(ss.sparkContext)
    val strategyBroadcast = ss.sparkContext.broadcast(strategy)
    strategyBroadcast.value.transform(dataStoreDFs)
  }

}
