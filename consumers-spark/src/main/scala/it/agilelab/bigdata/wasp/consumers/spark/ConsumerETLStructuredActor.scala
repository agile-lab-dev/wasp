//package it.agilelab.bigdata.wasp.consumers.spark
//
//import it.agilelab.bigdata.wasp.core.bl._
//
//class ConsumerETLStructuredActor(env: {
//  val topicBL: TopicBL; val indexBL: IndexBL; val rawBL: RawBL;
//  val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL
//}, sparkWriterFactory: SparkWriterFactory, streamingReader: StreamingReader, ssc: StreamingContext, etl: ETLModel, listener: ActorRef, plugins: Map[String, WaspConsumerSparkPlugin])
//    extends Actor
//    with Logging {
//  case object StreamReady
//
//  /*
//   * Actor methods start
//   */
//
//  def receive: Actor.Receive = {
//    case StreamReady => listener ! OutputStreamInitialized
//  }
//
//  // TODO check if mainTask has really to be invoked here
//  override def preStart(): Unit = {
//    super.preStart()
//    logger.info(s"Actor is transitioning from 'uninitialized' to 'initialized'")
//    validationTask()
//    mainTask()
//  }
//
//  /*
//   * Actor methods end
//   */
//
//  /**
//    * Strategy object initialize
//    */
//  //TODO: identical to it.agilelab.bigdata.wasp.consumers.spark.batch.BatchJobActor.createStrategy, externalize
//  private lazy val createStrategy: Option[Strategy] = etl.strategy match {
//    case None => None
//    case Some(strategyModel) =>
//      val result = Class
//        .forName(strategyModel.className)
//        .newInstance()
//        .asInstanceOf[Strategy]
//      result.configuration = strategyModel.configurationConfig() match {
//        case None                => ConfigFactory.empty()
//        case Some(configuration) => configuration
//      }
//
//      logger.info("strategy: " + result)
//      Some(result)
//  }
//
//  /**
//    * Index readers initialization
//    */
//  private def indexReaders(): List[StaticReader] = {
//    val defaultDataStoreIndexed =
//      ConfigManager.getWaspConfig.defaultIndexedDatastore
//    etl.inputs.flatMap({
//      case ReaderModel(name, endpointId, readerType) =>
//        val readerProduct = readerType.getActualProduct
//        logger.info(
//          s"Get index reader plugin $readerProduct before was $readerType, plugin map: $plugins")
//
//        val readerPlugin = plugins.get(readerProduct)
//        if (readerPlugin.isDefined) {
//          Some(
//            readerPlugin.get.getSparkReader(endpointId.getValue.toHexString,
//                                            name))
//        } else {
//          //TODO Check if readerType != topic
//          logger.warn(
//            s"The $readerProduct plugin in indexReaders does not exists")
//          None
//        }
//    })
//  }
//
//  /**
//    * Raw readers initialization
//    */
//  private def rawReaders(): List[StaticReader] =
//    etl.inputs
//      .flatMap({
//        case ReaderModel(name, endpointId, readerType) =>
//          logger.info(
//            s"Get raw reader plugin $readerType, plugin map: $plugins")
//          val readerPlugin = plugins.get(readerType.getActualProduct)
//          if (readerPlugin.isDefined) {
//            Some(
//              readerPlugin.get.getSparkReader(endpointId.getValue.toHexString,
//                                              name))
//          } else {
//            //TODO Check if readerType != topic
//            logger.error(
//              s"The $readerType plugin in rawReaders does not exists")
//            None
//          }
//      })
//
//  // TODO unify readers initialization (see BatchJobActor)
//  /**
//    * All static readers initialization
//    *
//    * @return
//    */
//  private def staticReaders(): List[StaticReader] =
//    indexReaders() ++ rawReaders()
//
//  /**
//    * Topic models initialization
//    */
//  private def topicModels(): List[Option[TopicModel]] =
//    etl.inputs
//      .flatMap({
//        case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) =>
//          val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
//          Some(topicOpt)
//        case _ => None
//      })
//
//  private def validationTask(): Unit = {
//    etl.inputs.foreach({
//      case ReaderModel(name, endpointId, ReaderType.kafkaReaderType) => {
//        val topicOpt = env.topicBL.getById(endpointId.getValue.toHexString)
//        if (topicOpt.isEmpty) {
//          //TODO Better exception
//          throw new Exception(s"There isn't this topic: $endpointId, $name")
//        }
//      }
//      case ReaderModel(name, endpointId, readerType) => {
//        val readerPlugin = plugins.get(readerType.getActualProduct)
//        if (readerPlugin.isDefined) {
//          readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name)
//        } else {
//          //TODO Better exception
//          logger.error(
//            s"There isn't the plugin for this index: '$endpointId', '$name', readerType: '$readerType'")
//          throw new Exception(s"There isn't this index: $endpointId, $name")
//        }
//      }
//    })
//    val topicReaderModelNumber =
//      etl.inputs.count(_.readerType.category == TopicModel.readerType)
//    if (topicReaderModelNumber == 0)
//      throw new Exception(
//        "There is NO topic to read data, inputs: " + etl.inputs)
//    if (topicReaderModelNumber != 1)
//      throw new Exception("MUST be only ONE topic, inputs: " + etl.inputs)
//  }
//  //TODO move in the extender class
//  def mainTask(): Unit = {
//
//    val topicStreams: List[(ReaderKey, DStream[String])] =
//      topicModels().map(topicModelOpt => {
//        assert(topicModelOpt.isDefined)
//
//        val topicModel = topicModelOpt.get
//        val stream: DStream[String] =
//          streamingReader.createStream(etl.group,
//                                       etl.kafkaAccessType,
//                                       topicModel)(ssc = ssc)
//        (ReaderKey(TopicModel.readerType, topicModel.name), stream)
//      })
//
//    //TODO  Join condition between streams
//
//    assert(topicStreams.nonEmpty)
//    assert(topicStreams.size == 1)
//
//    val topicStreamWithKey: (ReaderKey, DStream[String]) = topicStreams.head
//
//    val outputStream =
//      if (createStrategy.isDefined) {
//        val strategy = createStrategy.get
//
//        //TODO cache or reading?
//        // Reading static source to DF
//        val dataStoreDFs: Map[ReaderKey, DataFrame] =
//          staticReaders()
//            .map(staticReader => {
//
//              val dataSourceDF = staticReader.read(ssc.sparkContext)
//              (ReaderKey(staticReader.readerType, staticReader.name),
//               dataSourceDF)
//            })
//            .toMap
//
//        val mlModelsDB = new MlModelsDB(env)
//        // --- Broadcast models initialization ----
//        // Reading all model from DB and create broadcast
//        val mlModelsBroadcast: MlModelsBroadcastDB =
//          mlModelsDB.createModelsBroadcast(etl.mlModels)(ssc.sparkContext)
//
//        // Initialize the mlModelsBroadcast to strategy object
//        strategy.mlModelsBroadcast = mlModelsBroadcast
//        transform(topicStreamWithKey._1,
//                  topicStreamWithKey._2,
//                  dataStoreDFs,
//                  strategy)
//      } else {
//        topicStreamWithKey._2
//      }
//
//    val sparkWriterOpt =
//      sparkWriterFactory.createSparkWriterStreaming(env, ssc, etl.output)
//    sparkWriterOpt.foreach(w => {
//      w.write(outputStream)
//    })
//
//    // For some reason, trying to send directly a message from here to the guardian is not working ...
//    // NOTE: Maybe because mainTask is invoked in preStart ?
//    // TODO check required
//    logger.info(s"Actor is notifying the guardian that it's ready")
//    self ! StreamReady
//  }
//
//  private def transform(readerKey: ReaderKey,
//                        stream: DStream[String],
//                        dataStoreDFs: Map[ReaderKey, DataFrame],
//                        strategy: Strategy): DStream[String] = {
//    val sqlContext = SQLContextSingleton.getInstance(ssc.sparkContext)
//    val strategyBroadcast = ssc.sparkContext.broadcast(strategy)
//    stream.transform(rdd => {
//      //TODO Verificare se questo è il comportamento che si vuole
//      val dataframeToTransform = sqlContext.read.json(rdd)
//      if (dataframeToTransform.schema.nonEmpty) {
//
//        val completeMapOfDFs
//          : Map[ReaderKey, DataFrame] = dataStoreDFs + (readerKey -> dataframeToTransform)
//        strategyBroadcast.value.transform(completeMapOfDFs).toJSON.rdd
//
//      } else {
//        rdd
//      }
//    })
//  }
//
//}