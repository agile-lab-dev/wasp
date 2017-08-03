package it.agilelab.bigdata.wasp.consumers.spark.batch

import akka.actor.{Actor, ActorLogging, ActorRef}
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.readers.{HDFSReader, IndexReader, RawReader, StaticReader}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkWriter, SparkWriterFactory}
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models._
import org.apache.commons.lang.exception.ExceptionUtils._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import reactivemongo.bson._

import scala.concurrent.{Await, Future}


object BatchJobActor {
  val name = "BatchJobActor"
}

class BatchJobActor(env: {val batchJobBL: BatchJobBL; val indexBL: IndexBL; val rawBL: RawBL;  val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
                    val classLoader: Option[ClassLoader] = None,
                    sparkWriterFactory: SparkWriterFactory,
                    sc: SparkContext) extends Actor with ActorLogging {

  val logger = WaspLogger(this.getClass.getName)

  var lastBatchMasterRef : ActorRef = _

  def receive: Actor.Receive = {
    case jobModel: BatchJobModel =>
      logger.info(s"Processing Batch Job ${jobModel.name} ...")

      lastBatchMasterRef = sender()

      changeBatchState(jobModel._id.get, "PROCESSING")

      val dfsMap = retrieveDFs(jobModel.etl.inputs)
      logger.info(s"DFs retrieved successfully!")

      val mlModelsDB = new MlModelsDB(env)
      logger.info(s"Start to get the models")
      val mlModelsFuture: Future[MlModelsBroadcastDB] =
        mlModelsDB.createModelsBroadcast(jobModel.etl.mlModels)(sc = sc)

      // Wait for the result
      val mlModelsBroadcast: MlModelsBroadcastDB =
        Await.result(mlModelsFuture,  timeout.duration)

      val strategy = createStrategy(jobModel.etl).map(s => {
        s.sparkContext = Some(sc)
        s
      })
      val resDf = applyStrategy(dfsMap, mlModelsBroadcast, strategy)
      log.info("Saving strategy models")
      val futuresWriteResult = mlModelsDB.write(mlModelsBroadcast.getModelsToSave)

      logger.info(s"Strategy for batch ${jobModel.name} applied successfully")

      var writeSuccess = false
      resDf match {
        case Some(res) =>
          try {
            writeSuccess = writeResult(res, jobModel.etl.output)
          } catch {
            case e: Exception =>
              logger.error(s"Batch job ${jobModel.name} has failed to save the results. Exception: ${getStackTrace(e)}")
              writeSuccess = false
          }//TODO: sistemare stringa quando useremo enum
        case None =>
          logger.warn(s"Batch job ${jobModel.name} has no result to be written.")
          writeSuccess = true
      }

      val modelsWriteResult = Await.result(futuresWriteResult, timeout.duration)
      val possibleError = modelsWriteResult.filter(_._1.inError)
      if (possibleError.nonEmpty) {
        writeSuccess = false
        log.error(s"MongoDB error saving the models. ${possibleError.map(_.toString())} ")
      }

      val jobResult = if (writeSuccess) {
        JobStateEnum.SUCCESSFUL
      } else {
        JobStateEnum.FAILED
      }
      changeBatchState(jobModel._id.get, jobResult)
      lastBatchMasterRef ! BatchJobProcessedMessage(jobModel._id.get.stringify, jobResult)


      logger.info(s"Batch Job ${jobModel.name} has been processed.")
  }

  private def retrieveDFs(readerModels: List[ReaderModel]) : Map[ReaderKey, DataFrame] = {
    val readers = staticReaders(readerModels)

    readers.map(reader => (ReaderKey(reader.get.name,reader.get.readerType), reader.get.read(sc))).toMap
    //TODO: check readerType : NO streams allowed
  }

  private def applyStrategy(dfsMap: Map[ReaderKey, DataFrame], mlModelsBroadcast: MlModelsBroadcastDB, strategy: Option[Strategy]) : Option[DataFrame] = strategy match{
    case None => None
    case Some(strategyModel) =>
      strategyModel.mlModelsBroadcast = mlModelsBroadcast
      val result = strategyModel.transform(dfsMap)
      Some(result)
  }

  private def writeResult(dataFrame: DataFrame, writerModel: WriterModel) : Boolean = {

    val spakWriterOpt: Option[SparkWriter] = sparkWriterFactory.createSparkWriterBatch(env, sc, writerModel = writerModel)

    if(spakWriterOpt.isDefined) {
      spakWriterOpt.get.write(dataFrame)
      true
    } else {
      logger.error("Invalid writer type")
      false
    }
  }

  //TODO: identico a metodo privato in ConsumerETLActor, esternalizzare
  private def createStrategy(etl: ETLModel) : Option[Strategy] = etl.strategy match {
    case None => None
    case Some(strategyModel) =>
      //val result = Class.forName(strategyModel.className).newInstance().asInstanceOf[Strategy]
      // classLoader.map(cl => cl.loadClass(producer.className))

      println(s"************************** ${classLoader.toString()} -> strategyModel: ${strategyModel.className}")

      val result = classLoader.map(cl => cl.loadClass(strategyModel.className)).get.newInstance().asInstanceOf[Strategy]

      //.getOrElse(Class.forName(strategyModel.className).newInstance()).asInstanceOf[Strategy]
      result.configuration = strategyModel.configuration match {
        case None => Map[String, Any]()
        case Some(configuration) =>
          implicit def reader = new BSONDocumentReader[Map[String, Any]] {
            def read(bson: BSONDocument) =
              bson.elements.map(tuple =>
                tuple._1 -> (tuple._2 match {
                  case s: BSONString => s.value
                  case b: BSONBoolean => b.value
                  case i: BSONInteger => i.value
                  case l: BSONLong => l.value
                  case d: BSONDouble => d.value
                  case o: Any => o.toString
                })).toMap
          }

          BSON.readDocument[Map[String, Any]](configuration)
      }

      logger.info("strategy: " + result)
      Some(result)
  }

  /**
   * Index readers initialization
   */
  private def indexReaders(readers: List[ReaderModel]): List[Option[StaticReader]] = {
    readers.flatMap({
      case ReaderModel(id, name, IndexModel.readerType) =>
        Some(IndexReader.create(env.indexBL, id.stringify, name))
      case _ => None
    })
  }
  
  /**
    * Raw readers initialization
    */
  private def rawReaders(readers: List[ReaderModel]): List[Option[StaticReader]] = {
    readers.flatMap({
      case ReaderModel(id, name, "raw") => Some(RawReader.create(env.rawBL, id.stringify, name))
      case _ => None
    })
  }
  
  // TODO unify readers initialization (see ConsumerEtlActor)
  /**
    * All static readers initialization
    *
    * @return
    */
  private def staticReaders(readers: List[ReaderModel]): List[Option[StaticReader]] = indexReaders(readers) ++ rawReaders(readers)

  private def changeBatchState(id: BSONObjectID, newState: String): Unit =
  {
    val jobFut = env.batchJobBL.getById(id.stringify)
    val job: Option[BatchJobModel] = Await.result(jobFut, timeout.duration)
    job match {
      case Some(jobModel) => env.batchJobBL.setJobState(jobModel, newState)
      case None => logger.error("BatchEndedMessage with invalid id found.")
    }

  }

}
