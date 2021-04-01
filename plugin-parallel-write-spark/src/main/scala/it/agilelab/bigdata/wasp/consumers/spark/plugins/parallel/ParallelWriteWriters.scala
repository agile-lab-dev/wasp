package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel

import com.google.gson.stream.MalformedJsonException
import com.squareup.okhttp.{MediaType, OkHttpClient, Request, RequestBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ParallelWriteModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{HadoopS3aUtil, HadoppS3Utils}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.GenericModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.mongodb.scala.bson.BsonDocument


class ParallelWriteSparkStructuredStreamingWriter(genericModel: GenericModel,
                                                  ss: SparkSession)
  extends SparkStructuredStreamingWriter  with Logging {


  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    implicit val formats = DefaultFormats
    val okHttpClient = new OkHttpClient()

    //TODO entity url should be retrieved using Platform Catalog
    val entityURL = "http://localhost:9999/writeExecutionPlan"  //host.docker.internal
    val parallelWriteModel: ParallelWriteModel =
      if (genericModel.kind == "parallelWrite") parse(genericModel.value.toJson).extract[ParallelWriteModel]
      else throw new IllegalArgumentException(s"""Expected value of GenericModel.kind is "parallelWrite", found ${genericModel.value}""")

    val request: Request = buildEntityRequest(entityURL, Serialization.write(parallelWriteModel.requestBody))


    // create and configure DataStreamWriter
    stream.writeStream.foreachBatch(

      (batch: DataFrame, batchId: Long) => {

        try {
          val responseBody = okHttpClient.newCall(request).execute().body()
          val pwBody = parse(responseBody.string()).extract[WriteExecutionPlanBody]

          val s3path: String = HadoppS3Utils.useS3aScheme(pwBody.writeUri).toString()
          val temporaryCredentials = pwBody.temporaryCredentials
            .getOrElse("w", throw new MalformedJsonException("Error while parsing write temporary credentials"))

          val conf = new HadoopS3aUtil(ss.sparkContext.hadoopConfiguration, temporaryCredentials).performBulkHadoopCfgSetup
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
