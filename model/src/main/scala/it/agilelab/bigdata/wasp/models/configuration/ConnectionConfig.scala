package it.agilelab.bigdata.wasp.models.configuration

case class ConnectionConfig(
                             protocol: String,
                             host: String,
                             port: Int = 0,
                             timeout: Option[Long] = None,
                             metadata: Option[Map[String, String]]
                           ) {

  override def toString: String = {
    var result = ""

    if (protocol != null && !protocol.isEmpty)
      result = protocol + "://"

    result = result + host

    if (port != 0)
      result = result + ":" + port

    result
  }
}
