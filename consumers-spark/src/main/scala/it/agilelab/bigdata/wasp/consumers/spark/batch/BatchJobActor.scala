package it.agilelab.bigdata.wasp.consumers.spark.batch

import akka.actor.{Actor, Props}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB, TransformerWithInfo}
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.batch.BatchJobActor.Tick
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkReader
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkWriter, SparkWriterFactory}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.SparkBatchConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object BatchJobActor {

  /**
    * Message sent from [BatchJobActor] via the scheduler to trigger requesting of jobs.
    */
  private case object Tick

  /**
    * Props for [[BatchJobActor]].
    * @param env The environment
    * @param sparkWriterFactory A spark writer factory
    * @param plugins The plugins
    * @param checkInterval The interval between requests for job from [[BatchJobActor]] to [[SparkConsumersBatchMasterGuardian]]
    * @return props for [[BatchJobActor]]
    */
  def props(env: {val batchJobBL: BatchJobBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
            sparkWriterFactory: SparkWriterFactory,
            plugins: Map[String, WaspConsumersSparkPlugin],
            checkInterval: FiniteDuration) = Props(new BatchJobActor(env, sparkWriterFactory, plugins, checkInterval))

}

class BatchJobActor private (env: {val batchJobBL: BatchJobBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
                    sparkWriterFactory: SparkWriterFactory,
                    plugins: Map[String, WaspConsumersSparkPlugin],
                    checkInterval: FiniteDuration)
  extends Actor
    with Logging
    with SparkBatchConfiguration {


  var sparkContext: SparkContext = _

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(checkInterval, self, BatchJobActor.Tick)(context.system.dispatcher)

    if (!SparkSingletons.initializeSpark(sparkBatchConfig)) {
      logger.warn("The spark context was already intialized: it might not be using the spark batch configuration!")
    }

    sparkContext = SparkSingletons.getSparkContext
  }


  private def stepEnsureReadersAreNotTopicBased(readers: Seq[ReaderModel]): Try[Seq[ReaderModel]] =
    readers.find(_.readerType.category == Datastores.topicCategory)
      .map { _ => Failure(new Exception("No stream readers allowed in batch jobs")) }
      .getOrElse(Success(readers))

  private def stepEnsureWritersAreNotTopicBased(writer: WriterModel): Try[WriterModel] =
    if (writer.writerType.category == Datastores.topicCategory) {
      Failure(new Exception("No stream readers allowed in batch jobs"))
    } else {
      Success(writer)
    }


  private def stepStaticReaders(readerModels: Seq[ReaderModel]): Try[Seq[SparkReader]] = {

    case class ReaderPlugin(name: String, endpoint: String, plugin: WaspConsumersSparkPlugin)

    val pluginsForReaderModels: Seq[ReaderPlugin] = readerModels.flatMap {
      case ReaderModel(name, endpointName, readerType) => {
        plugins.get(readerType.getActualProduct).map(ReaderPlugin(name, endpointName, _))
      }
    }

    val zero = Try(Seq.empty[SparkReader])

    pluginsForReaderModels.foldLeft(zero)((a, b) => a.flatMap(readers => Try {
      readers ++ Seq(b.plugin.getSparkReader(b.endpoint, b.name))
    }))

  }

  private def stepDataFramesForReaders(readers: Seq[SparkReader]): Try[Map[ReaderKey, DataFrame]] = {

    val zero = Try(Map.empty[ReaderKey, DataFrame])

    readers.foldLeft(zero)((a, b) => a.flatMap(dataFrames => Try {
      dataFrames + (ReaderKey(b.readerType, b.name) -> b.read(sparkContext))
    }))

  }

  private def stepCreateStrategy(etl: BatchETLModel, sparkContext: SparkContext): Try[Option[Strategy]] = etl.strategy match {
    case None => Success(None)
    case Some(strategyModel) =>

      val result = Try {
        Class.forName(strategyModel.className)
          .newInstance()
          .asInstanceOf[Strategy]
      }

      val configuration = strategyModel.configurationConfig().getOrElse(ConfigFactory.empty())

      result.map(_.configuration = configuration)

      result.map(_.sparkContext = Some(sparkContext))

      result.map(Some(_))
  }

  private def stepApplyStrategy(dataStoreDFs: Map[ReaderKey, DataFrame], mlModelsBroadcast: MlModelsBroadcastDB, maybeStrategy: Option[Strategy]): Try[Option[DataFrame]] = maybeStrategy match {
    case None => Success(None)
    case Some(strategy) =>
      strategy.mlModelsBroadcast = mlModelsBroadcast
      Try(Some(strategy.transform(dataStoreDFs)))
  }

  private def stepCreateMlModelsBroadcast(mlModelsDB: MlModelsDB, mlModelsInfo: List[MlModelOnlyInfo], sparkContext: SparkContext): Try[MlModelsBroadcastDB] =
    Try {
      mlModelsDB.createModelsBroadcast(mlModelsInfo)(sparkContext)
    }

  private def stepSaveMlModels(mlModelsDB: MlModelsDB, mlModelsBroadcastDB: MlModelsBroadcastDB): Try[List[TransformerWithInfo]] = Try {
    mlModelsDB.write(mlModelsBroadcastDB.getModelsToSave)
  }

  private def stepWriteResult(dataFrame: DataFrame, writerModel: WriterModel): Try[Any] = {

    val maybeWriter: Try[Option[SparkWriter]] = Try {
      sparkWriterFactory.createSparkWriterBatch(env, sparkContext, writerModel = writerModel)
    }

    maybeWriter.flatMap {
      case Some(writer) => Try(writer.write(dataFrame))
      case None => Success(None)
    }
  }

  def run(batchJobModel: BatchJobModel, batchJobInstanceModel: BatchJobInstanceModel): Try[Any] = {

    val mlModelsDB = new MlModelsDB(env)

    for {
      readerModels <- stepEnsureReadersAreNotTopicBased(batchJobModel.etl.inputs).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to retrieve reader models for job ${batchJobModel.name}", e))
      }
      writerModel <- stepEnsureWritersAreNotTopicBased(batchJobModel.etl.output).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to retrieve writer model for job ${batchJobModel.name}", e))
      }
      readers <- stepStaticReaders(readerModels).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to retrieve readers for job ${batchJobModel.name}", e))
      }
      inputDataFrames <- stepDataFramesForReaders(readers).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to create data frames for job ${batchJobModel.name}", e))
      }
      strategy <- stepCreateStrategy(batchJobModel.etl, sparkContext).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to create strategy for job ${batchJobModel.name}", e))
      }
      mlModelsBroadcast <- stepCreateMlModelsBroadcast(mlModelsDB, batchJobModel.etl.mlModels, sparkContext).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to create mlmodelsbroadcast for job ${batchJobModel.name}", e))
      }
      outputDataFrame <- stepApplyStrategy(inputDataFrames, mlModelsBroadcast, strategy).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to apply strategy for job ${batchJobModel.name}", e))
      }
      _ <- stepSaveMlModels(mlModelsDB, mlModelsBroadcast).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to save mlmodels for job ${batchJobModel.name}", e))
      }
      result <- outputDataFrame.map(stepWriteResult(_, writerModel))
        .getOrElse(Failure(new Exception("Failed to write")))
        .recoverWith {
          case e: Throwable => Failure(new Exception(s"Failed to write output for job ${batchJobModel.name}", e))
        }
    } yield result
  }

  override def receive: Actor.Receive = {

    case Tick =>
      context.parent ! SparkConsumersBatchMasterGuardian.GimmeOneJob

    case SparkConsumersBatchMasterGuardian.NoJobsAvailable => {
      context.system.scheduler.scheduleOnce(checkInterval, self, BatchJobActor.Tick)(context.system.dispatcher)
    }

    case SparkConsumersBatchMasterGuardian.Error => {
      context.system.scheduler.scheduleOnce(checkInterval, self, BatchJobActor.Tick)(context.system.dispatcher)
    }

    case SparkConsumersBatchMasterGuardian.Job(jobModel, jobInstance) => {
      run(jobModel, jobInstance) match {
        case Success(_) => {
          logger.info(s"SUCCESS: jobInstance: [${jobInstance.name}] jobModel: [${jobModel.name}] ")
          sender() ! SparkConsumersBatchMasterGuardian.JobSucceeded(jobModel, jobInstance)
        }
        case Failure(e) => {
          logger.error(s"FAILED: jobInstance: [${jobInstance.name}] jobModel: [${jobModel.name}] ", e)
          sender() ! SparkConsumersBatchMasterGuardian.JobFailed(jobModel, jobInstance, e)
        }
      }

      context.system.scheduler.scheduleOnce(checkInterval, self, BatchJobActor.Tick)(context.system.dispatcher)
    }
  }


}