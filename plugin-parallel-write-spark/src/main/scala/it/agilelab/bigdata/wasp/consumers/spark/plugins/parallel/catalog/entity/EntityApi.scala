package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntity.CorrelationId
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.ParallelWriteEntityJsonProtocol._

import java.net.URL
import okio.BufferedSink

case class EntityApi(override val baseUrl: URL) extends ParallelWriteEntity {
  private val CORRELATION_ID_HEADER              = "X-PLT-Correlation-Id"
  private val getWriteExecutionPlanEndpoint: URL = new URL(s"$baseUrl/writeExecutionPlan").toURI.normalize().toURL
  private val getDataStreamEndpoint: URL         = new URL(s"$baseUrl/data/stream").toURI.normalize().toURL
  private val postDataCompleteEndpoint: URL      = new URL(s"$baseUrl/data/complete").toURI.normalize().toURL
  private val getDataCommittedEndpoint: URL      = new URL(s"$baseUrl/data/committed").toURI.normalize().toURL

  def getWriteExecutionPlan(
      requestBody: WriteExecutionPlanRequestBody,
      correlationId: CorrelationId
  ): WriteExecutionPlanResponseBody = {
    val headers = Map(CORRELATION_ID_HEADER -> correlationId.value)
    post(getWriteExecutionPlanEndpoint, Some(requestBody), headers)(
      writeExecutionPlanRequestBodyFormat,
      writeExecutionPlanResponseBodyFormat
    )
  }

  def postDataStream(requestBody: DataStreamRequestBody, correlationId: CorrelationId): Unit = {
    val headers = Map(CORRELATION_ID_HEADER -> correlationId.value)
    postStream(
      getDataStreamEndpoint,
      (sink: BufferedSink) => {
        sink.writeUtf8("[")
        while (requestBody.stream.hasNext) {
          sink.writeUtf8(requestBody.stream.next())
          if (requestBody.stream.hasNext) {
            sink.writeUtf8(",")
          }
        }
        sink.writeUtf8("]")
      },
      headers
    )
  }

  def postDataComplete(requestBody: DataCompleteRequestBody, correlationId: CorrelationId): Unit = {
    val headers = Map(CORRELATION_ID_HEADER -> correlationId.value)
    postComplete(postDataCompleteEndpoint, Some(requestBody), headers)(
      dataCompleteRequestBodyFormat
    )
  }

  def getDataCommitted(correlationId: CorrelationId): DataCommittedResponseBody = {
    val headers = Map(CORRELATION_ID_HEADER -> correlationId.value)
    get(getDataCommittedEndpoint, headers)(dataCommittedResponseBodyFormat)
  }
}
