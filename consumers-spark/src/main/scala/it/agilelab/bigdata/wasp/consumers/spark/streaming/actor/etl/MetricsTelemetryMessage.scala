package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, RootJsonFormat}


/**
  * @author andreaL
  */
case class MetricsTelemetryMessage (
                                     messageId:String,
                                     sourceId:String,
                                     metric:String,
                                     value:Long,
                                     timestamp:String
                                   )


object MetricsTelemetryMessageFormat extends SprayJsonSupport with DefaultJsonProtocol{

  implicit lazy val metricsTelemetryMessageFormat: RootJsonFormat[MetricsTelemetryMessage] = jsonFormat5(MetricsTelemetryMessage.apply)

}

