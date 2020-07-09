package it.agilelab.bigdata.wasp.models.configuration

case class ZookeeperConnectionsConfig(connections: Seq[ConnectionConfig], chRoot: String) {
  override def toString = connections.map(conn => s"${conn.host}:${conn.port}").mkString(",") + s"${chRoot}"
}
