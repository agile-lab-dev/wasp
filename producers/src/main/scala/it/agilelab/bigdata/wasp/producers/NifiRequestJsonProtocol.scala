package it.agilelab.bigdata.wasp.producers

import spray.json.DefaultJsonProtocol

object NifiRquestJsonProtocol extends DefaultJsonProtocol {
  implicit def nifiPlatform = jsonFormat2(NifiPlatform.apply)
  implicit def nifiRequest = jsonFormat3(NifiRequest.apply)
  implicit def nifiConfiguration = jsonFormat2(Configuration.apply)
}

case class NifiRequest(action: String, id: Option[String], child: Option[List[NifiPlatform]])
case class NifiPlatform(id: String, edge: Option[List[String]])
case class Configuration(routingInfo: NifiRequest, data: Option[Array[Byte]])