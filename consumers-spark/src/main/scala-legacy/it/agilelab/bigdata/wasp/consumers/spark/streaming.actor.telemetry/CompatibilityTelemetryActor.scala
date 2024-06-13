package it.agilelab.bigdata.wasp.consumer.spark.streaming.actor.telemetry

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.telemetry.TelemetryActor
import it.agilelab.bigdata.wasp.core.messages.TelemetryMessageJsonProtocol._
import it.agilelab.bigdata.wasp.core.messages.TelemetryMessageSourcesSummary
import scala.util.parsing.json.{JSONFormat, JSONObject}
import spray.json._



trait CompatibilityTelemetryActor {
  self: TelemetryActor =>
  @com.github.ghik.silencer.silent("deprecated")
  protected def toMessage(message: Any): String = {
    message match {
      case data: Map[_, _] =>
        JSONObject(data.asInstanceOf[Map[String, Any]]).toString(JSONFormat.defaultFormatter)
      case data: TelemetryMessageSourcesSummary => data.toJson.toString()
    }
  }
}
