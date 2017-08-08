package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.{Actor, ActorLogging, ActorRef, actorRef2Scala}
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.readers.{IndexReader, RawReader, StaticReader, StreamingReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactory
import it.agilelab.bigdata.wasp.core.WaspEvent.OutputStreamInitialized
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.MongoDBHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream



class ConsumerEtlActor(env: {val topicBL: TopicBL; val indexBL: IndexBL; val rawBL : RawBL; val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
                       sparkWriterFactory: SparkWriterFactory,
                       streamingReader: StreamingReader,
                       ssc: StreamingContext,
                       etl: ETLModel,
                       listener: ActorRef
                        ) extends Actor with ActorLogging  {

  val logger = WaspLogger(this.getClass.getName)

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
    log.info(s"Actor is transitioning from 'uninitialized' to 'initialized'")
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
  private lazy val createStrategy: Option[Strategy] = etl.strategy match {
    case None => None
    case Some(strategyModel) => {
      val result = Class.forName(strategyModel.className).newInstance().asInstanceOf[Strategy]
      result.configuration = strategyModel.configuration match {
        case None => Map[String, Any]()
        case Some(configuration) => MongoDBHelper.bsonDocumentToMap(configuration)
      }

      log.info("strategy: " + result)
      Some(result)
    }
  }
  
  /**
   * Index readers initialization
   */
  private def indexReaders(): List[Option[StaticReader]] = etl.inputs
    .flatMap({
    case ReaderModel(id, name, IndexModel.readerType) =>
      Some(IndexReader.create(env.indexBL, id.getValue.toHexString, name))
    case _ => None
  })
  
  /**
    * Raw readers initialization
    */
  private def rawReaders(): List[Option[StaticReader]] = etl.inputs
    .flatMap({
      case ReaderModel(id, name, "raw") => Some(RawReader.create(env.rawBL, id.getValue.toHexString, name))
      case _ => None
    })
  
  // TODO unify readers initialization (see BatchJobActor)
  /**
   * All static readers initialization
    *
    * @return
   */
  private def staticReaders(): List[Option[StaticReader]] = indexReaders() ++ rawReaders()

  /**
   * Topic models initialization
   */
  private def topicModels(): List[Option[TopicModel]] = etl.inputs
    .flatMap({
    case ReaderModel(id, name, TopicModel.readerType) =>
      val topicOpt =  env.topicBL.getById(id.getValue.toHexString)
      Some(topicOpt)
    case _ => None
  })


  private def validationTask(): Unit = {
    etl.inputs.foreach({
      case ReaderModel(id, name, IndexModel.readerType) => {
        val readerOpt = IndexReader.create(env.indexBL, id.getValue.toHexString, name)
        if (readerOpt.isEmpty) {
          //TODO Better exception
          throw new Exception(s"There isn't this index: $id, $name")
        }
      }

      case ReaderModel(id, name, TopicModel.readerType) => {
        val topicOpt = env.topicBL.getById(id.getValue.toHexString)
        if (topicOpt.isEmpty) {
          //TODO Better exception
          throw new Exception(s"There isn't this topic: $id, $name")
        }
      }
    })
    val topicReaderModelNumber = etl.inputs.count(_.readerType == TopicModel.readerType)
    if (topicReaderModelNumber == 0) throw new Exception("There is NO topic to read data")
    if (topicReaderModelNumber != 1) throw new Exception("MUST be only ONE topic")
  }
  //TODO move in the extender class
  def mainTask(): Unit = {

    val topicStreams: List[(ReaderKey, DStream[String])] = topicModels().map(topicModelOpt => {
      assert(topicModelOpt.isDefined)

      val topicModel = topicModelOpt.get
      val stream: DStream[String] = streamingReader.createStream(etl.group, etl.kafkaAccessType, topicModel)(ssc = ssc)
      (ReaderKey(TopicModel.readerType, topicModel.name), stream)
    })

    //TODO  Join condition between streams

    assert(topicStreams.nonEmpty)
    assert(topicStreams.size == 1)

    val topicStreamWithKey: (ReaderKey, DStream[String]) = topicStreams.head

    val outputStream =
      if (createStrategy.isDefined) {
        val strategy = createStrategy.get

        //TODO cache or reading?
        // Reading static source to DF
        val dataStoreDFs: Map[ReaderKey, DataFrame] =
          staticReaders().map(maybeStaticReader => {
            assert(maybeStaticReader.isDefined)

            val staticReader = maybeStaticReader.get
            val dataSourceDF = staticReader.read(ssc.sparkContext)
            (ReaderKey(staticReader.readerType, staticReader.name), dataSourceDF)
          }).toMap

        val mlModelsDB = new MlModelsDB(env)
        // --- Broadcast models initialization ----
        // Reading all model from DB and create broadcast
        val mlModelsBroadcast: MlModelsBroadcastDB = mlModelsDB.createModelsBroadcast(etl.mlModels)(ssc.sparkContext)

        // Initialize the mlModelsBroadcast to strategy object
        strategy.mlModelsBroadcast = mlModelsBroadcast
        transform(topicStreamWithKey._1, topicStreamWithKey._2, dataStoreDFs, strategy)
      } else {
        topicStreamWithKey._2
      }

    val sparkWriterOpt = sparkWriterFactory.createSparkWriterStreaming(env, ssc, etl.output)
    sparkWriterOpt.foreach(w => {
     w.write(outputStream)
    })

    // For some reason, trying to send directly a message from here to the guardian is not working ...
    // NOTE: Maybe because mainTask is invoked in preStart ? 
    // TODO check required
    log.info(s"Actor is notifying the guardian that it's ready")
    self ! StreamReady
  }

  private def transform(readerKey: ReaderKey,
                        stream: DStream[String],
                        dataStoreDFs: Map[ReaderKey, DataFrame],
                        strategy: Strategy): DStream[String] = {
    val sqlContext = SQLContextSingleton.getInstance(ssc.sparkContext)
    val strategyBroadcast = ssc.sparkContext.broadcast(strategy)
    stream.transform(rdd => {
      //TODO Verificare se questo è il comportamento che si vuole
      val dataframeToTransform = sqlContext.read.json(rdd)
      if (dataframeToTransform.schema.nonEmpty) {

        val completeMapOfDFs: Map[ReaderKey, DataFrame] = dataStoreDFs + (readerKey -> dataframeToTransform)
        strategyBroadcast.value.transform(completeMapOfDFs).toJSON

      } else {
        rdd
      }
    })
  }

}