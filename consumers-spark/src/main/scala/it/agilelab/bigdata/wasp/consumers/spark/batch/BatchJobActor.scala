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

  override def receive: Actor.Receive = {
    case jobModel: BatchJobModel =>
      logger.info(s"Processing Batch Job ${jobModel.name} ...")

      lastBatchMasterRef = sender()

      changeBatchState(jobModel._id.get, JobStateEnum.PROCESSING)

      // check if at least one stream reader is found
      val existTopicCategoryReaders = jobModel.etl.inputs.exists(r => r.readerType.category == Datastores.topicCategory)
      if (existTopicCategoryReaders) {
        // abort processing
        logger.error("No stream readers are allowed in batch jobs!")
        changeBatchState(jobModel._id.get, JobStateEnum.FAILED)
      } else {
        // check if the writer is stream
        val isTopicCategoryWriter = jobModel.etl.output.writerType.category == Datastores.topicCategory
        if (isTopicCategoryWriter) {
          // abort processing
          logger.error("No stream writer is allowed in batch jobs!")
          changeBatchState(jobModel._id.get, JobStateEnum.FAILED)
        } else {
          // implicit filtered with the check above which blocks using stream readers
          val staticReaders = jobModel.etl.inputs /*.filterNot(_.readerType.category == Datastores.topicCategory)*/

          val dataStoreDFs: Map[ReaderKey, DataFrame] =
            if (staticReaders.isEmpty) {
              logger.warn("Readers list empty!") // also print a warning when no static readers are defined
              Map.empty
            }
            else
              retrieveDFs(staticReaders)

          val nDFrequired = staticReaders.size
          val nDFretrieved = dataStoreDFs.size
          if (nDFretrieved != nDFrequired) {
            val error = "DFs not retrieved successfully!\n" +
              s"$nDFrequired DFs required - $nDFretrieved DFs retrieved!\n" +
              dataStoreDFs.toString
            logger.error(error)

            // abort processing
            changeBatchState(jobModel._id.get, JobStateEnum.FAILED)
          }
          else {
            if (!dataStoreDFs.isEmpty)
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

            val jobResult =
              if (writeMlModelsSuccess && writeOutputSuccess)
                JobStateEnum.SUCCESSFUL
              else
                JobStateEnum.FAILED

            changeBatchState(jobModel._id.get, jobResult)

            lastBatchMasterRef ! BatchJobProcessedMessage(jobModel._id.get.getValue.toHexString, jobResult)

            logger.info(s"Batch Job ${jobModel.name} has been processed. Result: $jobResult")
          }
        }
      }
  }

  private def retrieveDFs(staticReaderModels: List[ReaderModel]) : Map[ReaderKey, DataFrame] = {
    // Reading static source to DF
    retrieveStaticReaders(staticReaderModels)
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

  /**
    * Strategy object initialization
    */
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

  private def changeBatchState(id: BsonObjectId, newState: String): Unit =
  {
    val job = env.batchJobBL.getById(id.getValue.toHexString)
    job match {
      case Some(jobModel) => env.batchJobBL.setJobState(jobModel, newState)
      case None => logger.error("BatchEndedMessage with invalid id found.")
    }
  }
}