package it.agilelab.bigdata.wasp.producers

import spray.json.{DefaultJsonProtocol}

case class NifiRequest(action: String, id: Option[String], child: Option[List[NifiPlatform]], data: Option[Array[Byte]])
case class NifiPlatform(id: String, edge: Option[List[String]])

object NifiRquestJsonProtocol extends DefaultJsonProtocol {
  implicit def nifiPlatform = jsonFormat2(NifiPlatform.apply)
  implicit def nifiRequest = jsonFormat4(NifiRequest.apply)
}