package it.agilelab.bigdata.wasp.core.messages

import akka.http.scaladsl.model.HttpMethod
import it.agilelab.bigdata.wasp.core.WaspMessage
import spray.json.JsValue

case object Start extends WaspMessage

case object Stop extends WaspMessage

case class ModelKey(name: String, version: String, timestamp: Long)

case class RestRequest(httpMethod: HttpMethod, data: JsValue, model: ModelKey) extends WaspMessage