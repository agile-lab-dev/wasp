package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, entity}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteFormat.ParallelWriteFormat
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{ParallelWriteFormat, WriteExecutionPlanResponseBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService

object ParallelWriterFactory {


  def getWriter(writerDetails: WriterDetails, writeExecutionPlan: WriteExecutionPlanResponseBody, entityDetails: CatalogCoordinates, catalogService: DataCatalogService): ParallelWriter = {
    writeExecutionPlan.writeType match {
      case "Cold" => {
        if (ParallelWriteFormat.values.map(_.toString).contains(writeExecutionPlan.format)) {
          val format = ParallelWriteFormat.withName(writeExecutionPlan.format)
          getColdWriter(format, writerDetails, entityDetails, catalogService)

        } else throw new IllegalArgumentException(s"Entity responded with parallel write format ${writeExecutionPlan.format}. \nFormat must be: ${ParallelWriteFormat.values.mkString(" or ")}")
      }
      case "Hot" => throw new IllegalArgumentException("Hot parallel write not yet supported")
      case _ => throw new IllegalArgumentException(s"Entity responded with parallel write type ${writeExecutionPlan.writeType} \nWrite type must be Hot or Cold")
    }
  }

  private def getColdWriter(format: entity.ParallelWriteFormat.Value, writerDetails: WriterDetails, entityDetails: CatalogCoordinates, catalogService: DataCatalogService) = {
    writerDetails match {
      case parallelWrite: ParallelWrite => getParallelWriter(format, parallelWrite, entityDetails, catalogService)
      case continuousUpdate: ContinuousUpdate => getContinuousUpdateWriter(format, continuousUpdate, entityDetails, catalogService)
      case _ => throw new IllegalArgumentException(s"WriterDetails must be a ParallelWriteDetails or a ContinuousUpdateDetails")
    }
  }

  private def getContinuousUpdateWriter(format: ParallelWriteFormat, details: ContinuousUpdate, entityDetails: CatalogCoordinates, catalogService: DataCatalogService): ParallelWriter = {
    format match {
      case ParallelWriteFormat.delta => ContinuousUpdateWriter(details, entityDetails, catalogService)
      case _ => throw new IllegalArgumentException("ContinuousUpdateWriter not supported with format different from delta")
    }
  }

  private def getParallelWriter(format: ParallelWriteFormat, parallelWrite: ParallelWrite, entityDetails: CatalogCoordinates, catalogService: DataCatalogService): ParallelWriter = {
    format match {
      case ParallelWriteFormat.parquet => ParquetParallelWriter(parallelWrite, entityDetails, catalogService)
      case ParallelWriteFormat.delta => DeltaParallelWriter(parallelWrite, entityDetails, catalogService)
    }
  }
}
