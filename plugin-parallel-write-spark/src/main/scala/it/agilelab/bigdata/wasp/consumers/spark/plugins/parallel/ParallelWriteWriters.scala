package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel

import com.google.gson.stream.MalformedJsonException
import com.squareup.okhttp.{MediaType, OkHttpClient, Request, RequestBody}
import it.agilelab.bigdata.microservicecatalog.entity.ParallelWriteEntity
import it.agilelab.bigdata.microservicecatalog.{MicroserviceCatalogBuilder, MicroserviceCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{HadoopS3aUtil, HadoppS3Utils}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.GenericModel
import net.liftweb.json._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class ParallelWriteSparkStructuredStreamingWriter(genericModel: GenericModel,
                                                  ss: SparkSession)
  extends SparkStructuredStreamingWriter  with Logging {


  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    implicit val formats = DefaultFormats

    //TODO entity url should be retrieved using Platform Catalog
    val parallelWriteModel: ParallelWriteModel =
      if (genericModel.kind == "parallelWrite") parse(genericModel.value.toJson).extract[ParallelWriteModel]
      else throw new IllegalArgumentException(s"""Expected value of GenericModel.kind is "parallelWrite", found ${genericModel.value}""")

    val platformCatalogService: MicroserviceCatalogService[ParallelWriteEntity] = MicroserviceCatalogBuilder.getMicroserviceCatalogService[ParallelWriteEntity]()
    val entity: ParallelWriteEntity = platformCatalogService.getMicroservice(parallelWriteModel.entityDetails)


    // create and configure DataStreamWriter
    stream.writeStream.foreachBatch(

      (batch: DataFrame, batchId: Long) => {

        try {
          val writeExecutionPlan = entity.getWriteExecutionPlan(parallelWriteModel.requestBody)
          val s3path: String = HadoppS3Utils.useS3aScheme(writeExecutionPlan.writeUri).toString()
          val temporaryCredentials = writeExecutionPlan.temporaryCredentials
            .getOrElse("w", throw new MalformedJsonException("Error while parsing write temporary credentials"))

          val conf = new HadoopS3aUtil(ss.sparkContext.hadoopConfiguration, temporaryCredentials, parallelWriteModel.s3aEndpoint).performBulkHadoopCfgSetup
          val partitions: List[String] = parallelWriteModel.partitionBy.getOrElse(Nil)


          logger.info(s"Writing microbatch with id: ${batchId}")
          batch.write
            .mode(parallelWriteModel.mode)
            .format(parallelWriteModel.format)
            .partitionBy(partitions:_*)
            .save(s3path)

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

  def buildEntityRequest(entityUrl: String, body: String): Request = {

    val requestBuilder= new Request.Builder().url(entityUrl)
    val requestBody = RequestBody.create(MediaType.parse("application/json"), body)
    val request = requestBuilder
      .method("POST", requestBody)
      .build()

    request
  }

}



final case class WriteExecutionPlanBody(writeUri: String, writeType: String, temporaryCredentials: Map[String, TemporaryCredential])
final case class TemporaryCredential(accessKeyID: String, secretKey: String, sessionToken: String)
