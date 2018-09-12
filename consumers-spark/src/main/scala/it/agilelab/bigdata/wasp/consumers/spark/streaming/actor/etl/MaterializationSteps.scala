package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps.WriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils.generateSpecificStructuredStreamingCheckpointDir
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian.generateUniqueComponentName
import it.agilelab.bigdata.wasp.core.models.configuration.SparkStreamingConfigModel
import it.agilelab.bigdata.wasp.core.models.{PipegraphModel, StructuredStreamingETLModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}

import scala.util.{Failure, Success, Try}

/**
  * Trait collecting operations to be composed to realize Materialization of a [[StructuredStreamingETLModel]]
  */
trait MaterializationSteps {

  /**
    * We need a writer factory
    */
  protected val writerFactory: WriterFactory

  /**
    * Performs materialization of a DataFrame activated from a [[StructuredStreamingETLModel]].
    *
    * @param etl The etl whose output dataFrame is being materialized
    * @param pipegraph The pipegraph containing the etl
    * @param dataFrame The [[DataFrame]] created by the activation step
    * @return The Materialized [[StreamingQuery]]
    */
  protected def materialize(etl: StructuredStreamingETLModel, pipegraph: PipegraphModel, dataFrame: DataFrame)
    : Try[StreamingQuery] =
    for {
      config <- retrieveSparkStreamingConfig.recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot retrieve Spark Streaming config for etl ${etl.name}", e))
    }
      checkpointDir <- generateCheckPointDir(etl, config, pipegraph).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot generate checkpoint dir name for etl ${etl.name}", e))
    }
      sparkStructuredStreamingWriter <- createWriter(etl).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot create Spark Structured Streaming writer for etl ${etl.name}", e))
    }
      dataStreamWriter <- write(sparkStructuredStreamingWriter, dataFrame).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot create DataStreamWriter for etl ${etl.name}", e))
    }
      streamingQuery <- startQuery(dataStreamWriter, config, etl, queryName(etl, pipegraph), checkpointDir).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot materialize etl ${etl.name}", e))
    }
  } yield streamingQuery

  private def generateCheckPointDir(etl: StructuredStreamingETLModel,
                                    sparkStreamingConfig: SparkStreamingConfigModel,
                                    pipegraph: PipegraphModel) = Try {
    generateSpecificStructuredStreamingCheckpointDir(pipegraph, etl)
  }

  private def retrieveSparkStreamingConfig = Try {
    ConfigManager.getSparkStreamingConfig
  }

  private def createWriter(etl: StructuredStreamingETLModel): Try[SparkStructuredStreamingWriter] = {
    Try(writerFactory(etl, etl.streamingOutput)) match {
      case Success(Some(writer)) => Success(writer)
      case Success(None) => Failure(new Exception("Could not instantiate writer"))
      case failure: Failure[Option[SparkStructuredStreamingWriter]] => Failure(failure.exception)
    }
  }

  private def write(writer: SparkStructuredStreamingWriter, dataFrame: DataFrame): Try[DataStreamWriter[Row]] =
    Try {
      writer.write(dataFrame)
    }

  private def startQuery(dataStreamWriter: DataStreamWriter[Row],
                         config: SparkStreamingConfigModel,
                         etl: StructuredStreamingETLModel,
                         queryName: String,
                         checkpointDir: String): Try[StreamingQuery] =
    Try {
      val triggerInterval = SparkUtils.getTriggerIntervalMs(config, etl)

      val trigger = Trigger.ProcessingTime(triggerInterval)

      dataStreamWriter
        .queryName(queryName)
        .option("checkpointLocation", checkpointDir)
        .trigger(trigger)
        .start()
    }

  private def queryName(etl: StructuredStreamingETLModel, pipegraph: PipegraphModel) = generateUniqueComponentName(pipegraph, etl)

}

object MaterializationSteps {

  /**
    * A function able to go from a [[WriterModel]] to an [[Option]] of [[SparkStructuredStreamingWriter]].
    *
    * The goal of this type is to abstract out the concrete implementation of this computation.
    */
  type WriterFactory = (StructuredStreamingETLModel, WriterModel) => Option[SparkStructuredStreamingWriter]
}