package it.agilelab.bigdata.wasp.producers

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class NifiRequest(action: String, id: Option[String], child: Option[List[NifiPlatform]], data: Option[Array[Byte]])
case class NifiPlatform(id: String, edge: Option[List[String]])
case class HttpRequestConfiguration(scheme: String, host: String, port: Int)
case class NifiProducerConfiguration(request: HttpRequestConfiguration, child: Option[List[NifiPlatform]])

object NifiRquestJsonProtocol extends DefaultJsonProtocol {

  implicit def nifiPlatform: RootJsonFormat[NifiPlatform] = jsonFormat2(NifiPlatform.apply)

  implicit def nifiRequest: RootJsonFormat[NifiRequest] = jsonFormat4(NifiRequest.apply)

  implicit def httpRequestConfiguration: RootJsonFormat[HttpRequestConfiguration] =
    jsonFormat3(HttpRequestConfiguration.apply)

  implicit def nifiProducerConfiguration: RootJsonFormat[NifiProducerConfiguration] =
    jsonFormat2(NifiProducerConfiguration.apply)
}