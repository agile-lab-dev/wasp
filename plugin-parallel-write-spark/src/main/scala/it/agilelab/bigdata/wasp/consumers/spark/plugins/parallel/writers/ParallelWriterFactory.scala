package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.microservicecatalog.entity
import it.agilelab.bigdata.microservicecatalog.entity.ParallelWriteFormat.ParallelWriteFormat
import it.agilelab.bigdata.microservicecatalog.entity.{ParallelWriteFormat, WriteExecutionPlanResponseBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model._

object ParallelWriterFactory {


  def getWriter(writerDetails: WriterDetails, writeExecutionPlan: WriteExecutionPlanResponseBody): ParallelWriter = {
    writeExecutionPlan.writeType match {
      case "Cold" => {
        if (ParallelWriteFormat.values.map(_.toString).contains(writeExecutionPlan.format)) {
          val format = ParallelWriteFormat.withName(writeExecutionPlan.format)
          getColdWriter(format, writerDetails)

        } else throw new IllegalArgumentException(s"Entity responded with parallel write format ${writeExecutionPlan.format}. \nFormat must be: ${ParallelWriteFormat.values.mkString(" or ")}")
      }
      case "Hot" => throw new IllegalArgumentException("Hot parallel write not yet supported")
      case _ => throw new IllegalArgumentException(s"Entity responded with parallel write type ${writeExecutionPlan.writeType} \nWrite type must be Hot or Cold")
    }
  }

  private def getColdWriter(format: entity.ParallelWriteFormat.Value, writerDetails: WriterDetails) = {
    writerDetails match {
      case parallelWrite: ParallelWrite => getParallelWriter(format, parallelWrite)
      case continuousUpdate: ContinuousUpdate => getContinuousUpdateWriter(format, continuousUpdate)
      case _ => throw new IllegalArgumentException(s"WriterDetails must be a ParallelWriteDetails or a ContinuousUpdateDetails")
    }
  }

  private def getContinuousUpdateWriter(format: ParallelWriteFormat, details: ContinuousUpdate): ParallelWriter = {
    format match {
      case ParallelWriteFormat.delta => ContinuousUpdateWriter(details)
      case _ => throw new IllegalArgumentException("ContinuousUpdateWriter not supported with format different from delta")
    }
  }

  private def getParallelWriter(format: ParallelWriteFormat, parallelWrite: ParallelWrite): ParallelWriter = {
    ColdParallelWriter(format.toString.toLowerCase(), parallelWrite)
  }
}
