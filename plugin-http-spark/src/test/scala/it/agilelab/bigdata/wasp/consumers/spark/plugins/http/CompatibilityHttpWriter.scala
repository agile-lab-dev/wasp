package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import org.apache.spark.sql.streaming.StreamingQueryException

object CompatibilityHttpWriter {
  def getMessageFromStreamingQException(ex: Option[StreamingQueryException]): String =
    ex.get.cause.getCause.getMessage
}
