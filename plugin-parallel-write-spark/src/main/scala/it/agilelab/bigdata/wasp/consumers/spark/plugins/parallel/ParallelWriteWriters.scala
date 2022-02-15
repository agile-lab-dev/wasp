package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{ParallelWriteEntity, WriteExecutionPlanRequestBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{MicroserviceCatalogBuilder, MicroserviceCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers.ParallelWriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row}


class ParallelWriteSparkStructuredStreamingWriter(parallelWriteModel: ParallelWriteModel, catalogService: DataCatalogService)
  extends SparkStructuredStreamingWriter  with Logging {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val platformCatalogService: MicroserviceCatalogService = MicroserviceCatalogBuilder.getMicroserviceCatalogService()
    val entity: ParallelWriteEntity = platformCatalogService.getMicroservice(parallelWriteModel.entityDetails)

    // create and configure DataStreamWriter
    stream.writeStream
      .foreachBatch(

      (batch: DataFrame, batchId: Long) => {

        try {
          val request = WriteExecutionPlanRequestBody("External")
          val writeExecutionPlan = entity.getWriteExecutionPlan(request)
          val writer = ParallelWriterFactory.getWriter(
            parallelWriteModel.writerDetails,
            writeExecutionPlan, parallelWriteModel.entityDetails,
            catalogService
          )

          logger.info(s"Writing microbatch with id: ${batchId}")
          writer.write(writeExecutionPlan, batch)
          logger.info(s"Successfully wrote microbatch with id: ${batchId}")

        }
        catch {
          case e: Exception => {
            logger.info(s"Failed writing microbatch ${batchId} on S3 \n${e.printStackTrace()}")
            throw e
          }
        }
      }
    )
  }


}



final case class WriteExecutionPlanBody(writeUri: String, writeType: String, temporaryCredentials: Map[String, TemporaryCredential])
final case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String)
