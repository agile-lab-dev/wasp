package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.MaterializationSteps.WriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkUtils.generateStructuredStreamingCheckpointDir
import it.agilelab.bigdata.wasp.consumers.spark.writers.{SparkStructuredStreamingWriter, SparkWriterFactory}
import it.agilelab.bigdata.wasp.core.bl.PipegraphBL
import it.agilelab.bigdata.wasp.core.consumers.BaseConsumersMasterGuadian.generateUniqueComponentName
import it.agilelab.bigdata.wasp.core.models.configuration.SparkStreamingConfigModel
import it.agilelab.bigdata.wasp.core.models.{PipegraphModel, StructuredStreamingETLModel, WriterModel}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

trait MaterializationSteps {

  val pipegraph : PipegraphModel
  val writerFactory: WriterFactory

  def materialize(etl: StructuredStreamingETLModel,pipegraph: PipegraphModel, dataFrame: DataFrame): Try[StreamingQuery] =
    for {
    config <- retrieveSparkStreamingConfig.recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot retrieve spark streaming config in etl ${etl.name}", e))
    }
    checkpointDir <- generateCheckPointDir(etl, config, pipegraph).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot create checkpoint dir for etl ${etl.name}", e))
    }
    writer <- createWriter(etl).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot create writer in etl ${etl.name}", e))
    }
    streamingQuery <- write(writer, dataFrame, queryName(etl, pipegraph), checkpointDir).recoverWith {
      case e: Throwable => Failure(new Exception(s"Cannot materialize etl ${etl.name}", e))
    }
  } yield streamingQuery

  private def generateCheckPointDir(etl: StructuredStreamingETLModel,
                                    sparkStreamingConfig: SparkStreamingConfigModel,
                                    pipegraph: PipegraphModel) = Try {
    generateStructuredStreamingCheckpointDir(sparkStreamingConfig, pipegraph, etl)
  }

  private def retrieveSparkStreamingConfig = Try {
    ConfigManager.getSparkStreamingConfig
  }

  private def createWriter(etl: StructuredStreamingETLModel): Try[SparkStructuredStreamingWriter] = {
    Try(writerFactory(etl.output)) match {
      case Success(Some(writer)) => Success(writer)
      case Success(None) => Failure(new Exception("Could not instantiate writer"))
      case failure: Failure[Option[SparkStructuredStreamingWriter]] => Failure(failure.exception)
    }
  }

  private def write(writer: SparkStructuredStreamingWriter, dataFrame: DataFrame, queryName: String,
                    checkpointDir: String): Try[StreamingQuery] =
    Try {
      writer.write(dataFrame, queryName, checkpointDir)
    }

  private def queryName(etl: StructuredStreamingETLModel, pipegraph: PipegraphModel) = generateUniqueComponentName(pipegraph, etl)


}

object MaterializationSteps {
  type WriterFactory = (WriterModel) => Option[SparkStructuredStreamingWriter]
}