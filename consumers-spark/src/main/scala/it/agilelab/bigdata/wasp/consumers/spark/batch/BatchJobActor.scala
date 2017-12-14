package it.agilelab.bigdata.wasp.consumers.spark.batch

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkWriter, SparkWriterFactory}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.BatchJobProcessedMessage
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.mongodb.scala.bson.BsonObjectId

object BatchJobActor {
  val name = "BatchJobActor"
}

class BatchJobActor(env: {val batchJobBL: BatchJobBL; val indexBL: IndexBL; val rawBL: RawBL;  val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
                    val classLoader: Option[ClassLoader] = None,
                    sparkWriterFactory: SparkWriterFactory,
                    sc: SparkContext,
                    plugins: Map[String, WaspConsumersSparkPlugin]) extends Actor with Logging {
  var lastBatchMasterRef : ActorRef = _

  def receive: Actor.Receive = {
    case jobModel: BatchJobModel =>
      logger.info(s"Processing Batch Job ${jobModel.name} ...")

      lastBatchMasterRef = sender()

      changeBatchState(jobModel._id.get, JobStateEnum.PROCESSING)

      val readers = jobModel.etl.inputs

      // check if at least one stream reader is found
      val existTopicCategoryReaders = readers.exists(r => r.readerType.category == Datastores.topicCategory)
      if (existTopicCategoryReaders) {
        // abort processing
        logger.error("No stream readers are allowed in batch jobs!")
        changeBatchState(jobModel._id.get, JobStateEnum.FAILED)
      }
      else {

        val dataStoreDFs : Map[ReaderKey, DataFrame] =

          // print a warning when no readers are defined
          if(readers.isEmpty) {
            logger.warn("Readers list empty!")
            Map.empty
          }
          else
            retrieveDFs(readers)

        val nDFrequired = readers.size
        val nDFretrieved = dataStoreDFs.size
        if(nDFretrieved != nDFrequired) {
          // abort processing
          logger.error("DFs not retrieved successfully!")
          logger.error(s"$nDFrequired DFs required - $nDFretrieved DFs retrieved!")
          logger.error(dataStoreDFs.toString())
          changeBatchState(jobModel._id.get, JobStateEnum.FAILED)
        }
        else {
          if(!dataStoreDFs.isEmpty)
            logger.info("DFs retrieved successfully!")

          val mlModelsDB = new MlModelsDB(env)
          logger.info("Start to get the models")
          val mlModelsBroadcast: MlModelsBroadcastDB = mlModelsDB.createModelsBroadcast(jobModel.etl.mlModels)(sc = sc)

          val strategy = createStrategy(jobModel.etl).map(s => {
            s.sparkContext = Some(sc)
            s
          })
          val resDf = applyStrategy(dataStoreDFs, mlModelsBroadcast, strategy)

          logger.info(s"Strategy for batch ${jobModel.name} applied successfully")

          logger.info(s"Saving batch job ${jobModel.name} ml models")

          var writeMlModelsSuccess = false
          try {
            val modelsWriteResult = mlModelsDB.write(mlModelsBroadcast.getModelsToSave)
            writeMlModelsSuccess = true
            logger.info(s"Successfully wrote ${modelsWriteResult.size} ml models for batch ${jobModel.name} to MongoDB")
          } catch {
            case e: Exception => {
              logger.error(s"MongoDB error saving the ml models for atch ${jobModel.name}", e)
            }
          }

          logger.info(s"Saving batch job ${jobModel.name} output")

          var writeOutputSuccess = false
          resDf match {
            case Some(res) =>
              try {
                writeOutputSuccess = writeResult(res, jobModel.etl.output)
                if (writeOutputSuccess) {
                  logger.info(s"Successfully wrote output for batch job ${jobModel.name}")
                } else {
                  logger.error(s"Failed to write output for batch job ${jobModel.name}")
                }
              } catch {
                case e: Exception => {
                  logger.error(s"Failed to write output for batch job ${jobModel.name}", e)
                  writeOutputSuccess = false
                }
              }
            case None =>
              logger.warn(s"Batch job ${jobModel.name} has no output to be written.")
              writeOutputSuccess = true
          }

          val jobResult = if (writeMlModelsSuccess && writeOutputSuccess) {
            JobStateEnum.SUCCESSFUL
          } else {
            JobStateEnum.FAILED
          }
          changeBatchState(jobModel._id.get, jobResult)

          lastBatchMasterRef ! BatchJobProcessedMessage(jobModel._id.get.getValue.toHexString, jobResult)

          logger.info(s"Batch Job ${jobModel.name} has been processed. Result: $jobResult")
        }
      }
  }

  private def retrieveDFs(readerModels: List[ReaderModel]) : Map[ReaderKey, DataFrame] = {
    // Reading static source to DF
    staticReaders(readerModels)
      .flatMap(staticReader => {
        try {
          val dataSourceDF = staticReader.read(sc)
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

  private def applyStrategy(dataStoreDFs: Map[ReaderKey, DataFrame], mlModelsBroadcast: MlModelsBroadcastDB, strategy: Option[Strategy]) : Option[DataFrame] = strategy match{
    case None => None
    case Some(strategyModel) =>
      strategyModel.mlModelsBroadcast = mlModelsBroadcast
      val result = strategyModel.transform(dataStoreDFs)
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

  private def createStrategy(etl: BatchETLModel): Option[Strategy] = etl.strategy match {
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
  private def indexReaders(readers: List[ReaderModel]): List[SparkReader] = {
    val defaultDataStoreIndexed = ConfigManager.getWaspConfig.defaultIndexedDatastore
    readers.flatMap({
      case ReaderModel(name, endpointId, readerType) =>
        val readerProduct = readerType.getActualProduct
        logger.info(s"Get index reader plugin $readerProduct before was $readerType, plugin map: $plugins")
        val readerPlugin = plugins.get(readerProduct)
        if (readerPlugin.isDefined) {
          Some(readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name))
        } else {
          logger.error(s"The $readerProduct plugin in indexReaders does not exists")
          None
        }
      case _ => None
    })
  }
  
  /**
    * Raw readers initialization
    */
  private def rawReaders(readers: List[ReaderModel]): List[SparkReader] = {
    readers.flatMap({
      case ReaderModel(name, endpointId, readerType) =>
        logger.info(s"Get raw reader plugin $readerType, plugin map: $plugins")
        val readerPlugin = plugins.get(readerType.getActualProduct)
        if (readerPlugin.isDefined) {
          Some(readerPlugin.get.getSparkReader(endpointId.getValue.toHexString, name))
        } else {
          logger.error(s"The $readerType plugin in rawReaders does not exists")
          None
        }
      case _ => None
    })
  }

  // TODO indexReaders() and rawReaders() are equals -> to call only once
  /**
    * All static readers initialization
    *
    * @return
    */
  private def staticReaders(readers: List[ReaderModel]): List[SparkReader] = indexReaders(readers) ++ rawReaders(readers)

  private def changeBatchState(id: BsonObjectId, newState: String): Unit =
  {
    val job = env.batchJobBL.getById(id.getValue.toHexString)
    job match {
      case Some(jobModel) => env.batchJobBL.setJobState(jobModel, newState)
      case None => logger.error("BatchEndedMessage with invalid id found.")
    }

  }

}
