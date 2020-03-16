package it.agilelab.bigdata.wasp.consumers.spark.batch

import java.io.{PrintStream, PrintWriter}

import akka.actor.{Actor, Props}
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB, TransformerWithInfo}
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.batch.BatchJobActor.Tick
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.SparkBatchReader
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{HasPostMaterializationHook, ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkBatchWriter, SparkWriterFactory}
import it.agilelab.bigdata.wasp.core.bl._
import it.agilelab.bigdata.wasp.core.datastores.{DatastoreProduct, TopicCategory}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, SparkBatchConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

case class AggregateException(msg: String, exs: Seq[Throwable]) extends Exception(msg) {

  override def printStackTrace(w: PrintStream): Unit = exs.foreach(_.printStackTrace(w))

  override def printStackTrace(w: PrintWriter): Unit = exs.foreach(_.printStackTrace(w))

  override def toString: String = exs.foldLeft(super.toString)((string, ex) => string + System.lineSeparator() + ex.toString)

}

object BatchJobActor {

  /**
    * Message sent from [BatchJobActor] via the scheduler to trigger requesting of jobs.
    */
  private case object Tick

  /**
    * Props for [[BatchJobActor]].
    *
    * @param env                The environment
    * @param sparkWriterFactory A spark writer factory
    * @param plugins            The plugins
    * @param checkInterval      The interval between requests for job from [[BatchJobActor]] to [[SparkConsumersBatchMasterGuardian]]
    * @return exitingWatchdogProps for [[BatchJobActor]]
    */
  def props(env: {val batchJobBL: BatchJobBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
            sparkWriterFactory: SparkWriterFactory,
            plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin],
            checkInterval: FiniteDuration) = Props(new BatchJobActor(env, sparkWriterFactory, plugins, checkInterval))

}

class BatchJobActor private(env: {val batchJobBL: BatchJobBL; val indexBL: IndexBL; val rawBL: RawBL; val keyValueBL: KeyValueBL; val mlModelBL: MlModelBL},
                            sparkWriterFactory: SparkWriterFactory,
                            plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin],
                            checkInterval: FiniteDuration)
  extends Actor
    with Logging
    with SparkBatchConfiguration {


  var sparkContext: SparkContext = _

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(checkInterval, self, BatchJobActor.Tick)(context.system.dispatcher)

    if (!SparkSingletons.initializeSpark(sparkBatchConfig, ConfigManager.getTelemetryConfig, ConfigManager.getKafkaConfig)) {
      logger.warn("The spark context was already initialized: it might not be using the spark batch configuration!")
    }

    sparkContext = SparkSingletons.getSparkContext
  }

  private def stepEnsureReadersAreNotTopicBased(readers: Seq[ReaderModel]): Try[Seq[ReaderModel]] =
    readers.find(_.datastoreProduct.isInstanceOf[TopicCategory])
      .map { _ => Failure(new Exception("No stream readers allowed in batch jobs")) }
      .getOrElse(Success(readers))

  private def logIfWriterIsTopicBased(writer: WriterModel): Try[WriterModel] = {
    writer.datastoreProduct match {
      case _: TopicCategory =>
        logger.warn("Writing from a Batch to kafka looks like an anti-pattern: use with caution")
        Success(writer)
      case _ => Success(writer)
    }
  }

  private def stepStaticReaders(readerModels: Seq[ReaderModel]): Try[Seq[SparkBatchReader]] = {

    case class ReaderPlugin(readerModel: ReaderModel, plugin: WaspConsumersSparkPlugin)

    val pluginsForReaderModels: Seq[ReaderPlugin] = readerModels.flatMap {
      readerModel => {
        plugins.get(readerModel.datastoreProduct).map(ReaderPlugin(readerModel, _))
      }
    }

    val zero = Try(Seq.empty[SparkBatchReader])

    pluginsForReaderModels.foldLeft(zero)((a, b) => a.flatMap(readers => Try {
      readers ++ Seq(b.plugin.getSparkBatchReader(sparkContext, b.readerModel))
    }))

  }

  private def stepDataFramesForReaders(readers: Seq[SparkBatchReader]): Try[Map[ReaderKey, DataFrame]] = {

    val zero = Try(Map.empty[ReaderKey, DataFrame])

    readers.foldLeft(zero)((a, b) => a.flatMap(dataFrames => Try {
      dataFrames + (ReaderKey(b.readerType, b.name) -> b.read(sparkContext))
    }))

  }

  private def stepApplyStrategy(dataStoreDFs: Map[ReaderKey, DataFrame], mlModelsBroadcast: MlModelsBroadcastDB, maybeStrategy: Option[Strategy]): Try[Option[DataFrame]] = maybeStrategy match {
    case None => Success(None)
    case Some(strategy) =>
      strategy.mlModelsBroadcast = mlModelsBroadcast
      Try(Some(strategy.transform(dataStoreDFs)))
  }

  private def stepSaveMlModels(mlModelsDB: MlModelsDB, mlModelsBroadcastDB: MlModelsBroadcastDB): Try[List[TransformerWithInfo]] = Try {
    mlModelsDB.write(mlModelsBroadcastDB.getModelsToSave)
  }

  private def stepWriteResult(dataFrame: DataFrame, writerModel: WriterModel): Either[(Option[DataFrame], Throwable), DataFrame] = {

    val maybeWriter: Either[(Option[DataFrame], Throwable), Option[SparkBatchWriter]] = Try {
      sparkWriterFactory.createSparkWriterBatch(env, sparkContext, writerModel = writerModel)
    } match {
      case Failure(exception) => Left(Option.empty[DataFrame] -> exception)
      case Success(value) => Right(value)
    }

    maybeWriter.right.flatMap {
      case Some(writer) => Try {
        writer.write(dataFrame)

      } match {
        case Failure(exception) => Left(Option(dataFrame) -> exception)
        case Success(_) => Right(dataFrame)
      }
      case None => Right(dataFrame)
    }
  }

  def run(batchJobModel: BatchJobModel, batchJobInstanceModel: BatchJobInstanceModel): Try[Unit] = {

    val mlModelsDB = new MlModelsDB(env)

    for {
      readerModels <- stepEnsureReadersAreNotTopicBased(batchJobModel.etl.inputs).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to retrieve reader models for job ${batchJobModel.name}", e))
      }
      writerModel <- logIfWriterIsTopicBased(batchJobModel.etl.output).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to retrieve writer model for job ${batchJobModel.name}", e))
      }
      readers <- stepStaticReaders(readerModels).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to retrieve readers for job ${batchJobModel.name}", e))
      }
      inputDataFrames <- stepDataFramesForReaders(readers).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to create data frames for job ${batchJobModel.name}", e))
      }
      strategy <- BatchJobEtlExecution.stepCreateStrategy(batchJobModel.etl, batchJobInstanceModel.restConfig, sparkContext).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to create strategy for job ${batchJobModel.name}", e))
      }

      postHookOnWriteSuccess = (s: DataFrame) => strategy match {
        case Some(hook: HasPostMaterializationHook) =>
          hook.postMaterialization(Some(s), None)
        case _ => Success(())
      }

      postHookOnWriteFailure = (f: (Option[DataFrame], Throwable)) => strategy match {
        case Some(hook: HasPostMaterializationHook) => hook.postMaterialization(f._1, Some(f._2))
        case _ => Failure(f._2)
      }

      mlModelsBroadcast <- BatchJobEtlExecution.stepCreateMlModelsBroadcast(mlModelsDB, batchJobModel.etl, sparkContext).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to create mlmodelsbroadcast for job ${batchJobModel.name}", e))
      }
      outputDataFrame <- stepApplyStrategy(inputDataFrames, mlModelsBroadcast, strategy).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to apply strategy for job ${batchJobModel.name}", e))
      }
      _ <- stepSaveMlModels(mlModelsDB, mlModelsBroadcast).recoverWith {
        case e: Throwable => Failure(new Exception(s"Failed to save mlmodels for job ${batchJobModel.name}", e))
      }
      result <- outputDataFrame.map(stepWriteResult(_, writerModel))
        .getOrElse(Left(None -> new Exception("Failed to write")))
        .fold(postHookOnWriteFailure, postHookOnWriteSuccess)
        .recoverWith { case e => Failure(new Exception(s"Failed to write output for job ${batchJobModel.name}", e)) }
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