package it.agilelab.bigdata.wasp.master.web.models

import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat}

case class RestProducerModel(httpMethod: String, data: JsValue, mlmodel: Option[String])

object RestProducerModelJsonProtocol extends DefaultJsonProtocol {
  implicit def nifiPlatform: RootJsonFormat[RestProducerModel] = jsonFormat3(RestProducerModel.apply)
}