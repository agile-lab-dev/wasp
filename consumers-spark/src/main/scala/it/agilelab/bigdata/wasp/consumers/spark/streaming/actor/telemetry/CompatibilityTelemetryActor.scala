package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.telemetry

import it.agilelab.bigdata.wasp.core.messages.TelemetryMessageJsonProtocol._
import it.agilelab.bigdata.wasp.core.messages.TelemetryMessageSourcesSummary
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, Extraction}
import spray.json._

trait CompatibilityTelemetryActor {
  self: TelemetryActor =>
  protected def toMessage(message: Any): String = {
    message match {
      case data: Map[_, _] =>
        implicit val formats: DefaultFormats.type = DefaultFormats
        compact(render(Extraction.decompose(data.asInstanceOf[Map[String, Any]])))
      case data: TelemetryMessageSourcesSummary => data.toJson.toString()
    }
  }
}
