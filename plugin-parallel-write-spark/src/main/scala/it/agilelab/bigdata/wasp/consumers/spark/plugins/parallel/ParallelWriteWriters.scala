package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel


import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{EntityCatalogBuilder, EntityCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity.randomCorrelationId
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.{
  ParallelWriteEntity,
  WriteExecutionPlanRequestBody
}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.DataCatalogService
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers.ParallelWriterFactory
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row}

class ParallelWriteSparkStructuredStreamingWriter(
    parallelWriteModel: ParallelWriteModel,
    catalogService: DataCatalogService
) extends SparkStructuredStreamingWriter
    with Logging {

  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    val platformCatalogService: EntityCatalogService = EntityCatalogBuilder.getEntityCatalogService()
    val entityApi: ParallelWriteEntity                        = platformCatalogService.getEntityApi(parallelWriteModel.entityDetails)
    // create and configure DataStreamWrite
    stream.writeStream.foreachBatch { (batch: DataFrame, batchId: Long) =>
      try {
        // add to random uuid the spark-batch-id, so to have a link between the microbatchID and the correlationID
        val correlationId      = randomCorrelationId()
        logger.info(s"Starting job with correlationID: ${correlationId.value}")
        val request            = WriteExecutionPlanRequestBody()
        val writeExecutionPlan = entityApi.getWriteExecutionPlan(request, correlationId)
        val writer = ParallelWriterFactory.getWriter(
          parallelWriteModel.writerDetails,
          writeExecutionPlan,
          parallelWriteModel.entityDetails,
          catalogService,
          entityApi
        )
        logger.info(s"Writing microbatch with id: $batchId")
        try {
          writer.write(writeExecutionPlan, batch, correlationId)
        } catch {
          case e: Exception =>
            logger.error("Failed writing a microbatch", e)
            writer.rollback(correlationId)
            throw e
        }
        writer.complete(correlationId)
        writer.commit(correlationId, batchId)
      } catch {
        case e: Exception =>
          logger.info(s"Failed writing microbatch $batchId", e)
          throw e
      }
    }
  }
}

final case class WriteExecutionPlanBody(
    writeUri: String,
    writeType: String,
    temporaryCredentials: Map[String, TemporaryCredential]
)

final case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String)
