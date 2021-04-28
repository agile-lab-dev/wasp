package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous

import com.google.gson.stream.MalformedJsonException
import it.agilelab.bigdata.microservicecatalog.entity.ParallelWriteEntity
import it.agilelab.bigdata.microservicecatalog.{MicroserviceCatalogBuilder, MicroserviceCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model.ContinuousUpdateModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.utils.{HadoopS3Utils, HadoopS3aUtil}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class ContinuousUpdateSparkStructuredStreamingWriter(writer: Writer,
                                                     continuousUpdateModel: ContinuousUpdateModel,
                                                     ss: SparkSession)
  extends SparkStructuredStreamingWriter  with Logging {


  override def write(stream: DataFrame): DataStreamWriter[Row] = {

    val platformCatalogService: MicroserviceCatalogService[ParallelWriteEntity] = MicroserviceCatalogBuilder.getMicroserviceCatalogService[ParallelWriteEntity]()
    val entity: ParallelWriteEntity = platformCatalogService.getMicroservice(continuousUpdateModel.entityDetails)
    // create and configure DataStreamWriter

    stream.writeStream
      .format("delta")
      .outputMode("append")
      .foreachBatch(

    (batch: DataFrame, batchId: Long) => {

      try {
        val writeExecutionPlan = entity.getWriteExecutionPlan(continuousUpdateModel.requestBody)

        val s3path: String = HadoopS3Utils.useS3aScheme(writeExecutionPlan.writeUri).toString()
        val temporaryCredentials = writeExecutionPlan.temporaryCredentials
          .getOrElse("w", throw new MalformedJsonException("Error while parsing write temporary credentials"))
            if (new HadoopS3aUtil(ss.sparkContext.hadoopConfiguration, temporaryCredentials, continuousUpdateModel.s3aEndpoint).performBulkHadoopCfgSetup.isFailure) throw new Exception("Failed Hadoop settings configuration")
            logger.info(s"Writing microbatch with id: ${batchId} using DeltaWriter")
            writer.write(s3path, batch)
            logger.info(s"Successfully wrote microbatch with id: ${batchId} using DeltaWriter")
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


