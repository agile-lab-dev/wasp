package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteFormat.ParallelWriteFormat
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{
  ParallelWriteEntity,
  ParallelWriteFormat,
  WriteExecutionPlanResponseBody
}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{entity, CatalogCoordinates}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService

object ParallelWriterFactory {

  val HOT = "Hot"
  val COLD = "Cold"

  def getWriter(
                 writerDetails: WriterDetails,
                 writeExecutionPlan: WriteExecutionPlanResponseBody,
                 entityDetails: CatalogCoordinates,
                 catalogService: DataCatalogService,
                 entityAPI: ParallelWriteEntity
               ): ParallelWriter = {
    writeExecutionPlan.writeType match {
      case COLD =>
        val writeFormat = writeExecutionPlan.format.getOrElse(
          throw new RuntimeException("Entity responded without a format field for a COLD case write"))
        if (ParallelWriteFormat.values.map(_.toString).contains(writeFormat)) {
          val format = ParallelWriteFormat.withName(writeFormat)
          getColdWriter(format, writerDetails, entityAPI, entityDetails, catalogService)
        } else
          throw new IllegalArgumentException(
            s"Entity responded with parallel write format ${writeExecutionPlan.format}. \nFormat must be: ${
              ParallelWriteFormat.values
                .mkString(" or ")
            }"
          )
      case HOT => getHotWriter(entityDetails, catalogService, entityAPI)
      case _ =>
        throw new IllegalArgumentException(
          s"Entity responded with parallel write type ${writeExecutionPlan.writeType} \nWrite type must be Hot or Cold"
        )
    }
  }

  private def getHotWriter(
                            entityDetails: CatalogCoordinates,
                            catalogService: DataCatalogService,
                            entityApi: ParallelWriteEntity
                          ) = {
    HotParallelWriter(entityDetails, catalogService, entityApi)
  }

  private def getColdWriter(
                             format: entity.ParallelWriteFormat.Value,
                             writerDetails: WriterDetails,
                             entityAPI: ParallelWriteEntity,
                             entityDetails: CatalogCoordinates,
                             catalogService: DataCatalogService
                           ) = {
    writerDetails match {
      case parallelWrite: ParallelWrite => getParallelWriter(format, parallelWrite, entityAPI, entityDetails, catalogService)
      case continuousUpdate: ContinuousUpdate =>
        getContinuousUpdateWriter(format, continuousUpdate, entityAPI, entityDetails, catalogService)
      case _ =>
        throw new IllegalArgumentException(s"WriterDetails must be a ParallelWriteDetails or a ContinuousUpdateDetails")
    }
  }

  private def getContinuousUpdateWriter(
                                         format: ParallelWriteFormat,
                                         details: ContinuousUpdate,
                                         entityAPI: ParallelWriteEntity,
                                         entityDetails: CatalogCoordinates,
                                         catalogService: DataCatalogService
                                       ): ParallelWriter = {
    format match {
      case ParallelWriteFormat.delta => ContinuousUpdateWriter(details, entityAPI, entityDetails, catalogService)
      case _ =>
        throw new IllegalArgumentException("ContinuousUpdateWriter not supported with format different from delta")
    }
  }

  private def getParallelWriter(
                                 format: ParallelWriteFormat,
                                 parallelWrite: ParallelWrite,
                                 entityAPI: ParallelWriteEntity,
                                 entityDetails: CatalogCoordinates,
                                 catalogService: DataCatalogService
                               ): ParallelWriter = {
    format match {
      case ParallelWriteFormat.parquet => ParquetParallelWriter(parallelWrite, entityAPI, entityDetails, catalogService)
      case ParallelWriteFormat.delta => DeltaParallelWriter(parallelWrite, entityAPI, entityDetails, catalogService)
    }
  }
}
