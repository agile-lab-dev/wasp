package it.agilelab.bigdata.wasp.master.web.models

import it.agilelab.bigdata.wasp.core.messages.ModelKey
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat}

case class RestProducerModel(httpMethod: String, data: JsValue, mlModel: ModelKey)

object RestProducerModelJsonProtocol extends DefaultJsonProtocol {

  implicit val modelKey = jsonFormat3(ModelKey)
  implicit def nifiPlatform: RootJsonFormat[RestProducerModel] = jsonFormat3(RestProducerModel.apply)
}