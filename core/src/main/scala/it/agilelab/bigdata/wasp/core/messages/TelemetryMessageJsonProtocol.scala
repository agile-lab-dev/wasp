package it.agilelab.bigdata.wasp.core.messages

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
  * @author Eugenio Liso
  */
object TelemetryMessageJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val telemetryMessageSource: RootJsonFormat[TelemetryMessageSource] = jsonFormat6(TelemetryMessageSource.apply)
  implicit val telemetryMessageSources: RootJsonFormat[TelemetryMessageSourcesSummary] = jsonFormat1(TelemetryMessageSourcesSummary.apply)
}
