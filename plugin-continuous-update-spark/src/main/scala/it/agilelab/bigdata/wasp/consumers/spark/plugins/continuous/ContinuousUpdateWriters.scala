package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous

import com.google.gson.stream.MalformedJsonException
import com.squareup.okhttp.{MediaType, OkHttpClient, Request, RequestBody}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model.ContinuousUpdateModel
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.utils.{HadoopS3Utils, HadoopS3aUtil}
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkStructuredStreamingWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import net.liftweb.json._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class ContinuousUpdateSparkStructuredStreamingWriter(writer: Writer,
                                                     continuousUpdateModel: ContinuousUpdateModel,
                                                     ss: SparkSession)
  extends SparkStructuredStreamingWriter  with Logging {


  override def write(stream: DataFrame): DataStreamWriter[Row] = {
    implicit val formats = DefaultFormats
    val okHttpClient = new OkHttpClient()

    //TODO entity url should be retrieved using Platform Catalog
//    val entityURL = "http://host.docker.internal:9999/writeExecutionPlan"  //host.docker.internal
    val entityURL = "http://localhost:9999/writeExecutionPlan"  //host.docker.internal

    val request: Request = buildEntityRequest(entityURL, Serialization.write(continuousUpdateModel.requestBody))
    // create and configure DataStreamWriter

    stream.writeStream
      .format("delta")
      .outputMode("append")
      .foreachBatch(

    (batch: DataFrame, batchId: Long) => {

      try {
        val responseBody = okHttpClient.newCall(request).execute().body()
        val pwBody = parse(responseBody.string()).extract[WriteExecutionPlanBody]

        val s3path: String = HadoopS3Utils.useS3aScheme(pwBody.writeUri).toString()
        val temporaryCredentials = pwBody.temporaryCredentials
          .getOrElse("w", throw new MalformedJsonException("Error while parsing write temporary credentials"))


            if (new HadoopS3aUtil(ss.sparkContext.hadoopConfiguration, temporaryCredentials).performBulkHadoopCfgSetup.isFailure) throw new Exception("Failed Hadoop settings configuration")
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
