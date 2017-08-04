package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, ConnectionConfig}
import org.mongodb.scala.bson.BsonObjectId


case class SolrConfigModel(
                            connections: Seq[ConnectionConfig],
                            apiEndPoint: Option[ConnectionConfig],
                            name: String,
                            _id: Option[BsonObjectId],
                            cluster_name: String
                          )


object SolrConfigModel {
  val default = SolrConfigModel(
    connections = Seq(ConnectionConfig("", "localhost", 8983, None, Some(Map.empty[String, String]))),
    apiEndPoint = Some(ConnectionConfig("http", "localhost", 8983, None, Some(Map("zookeeperRootNode" -> "/solr")))),
    ConfigManager.solrConfigName,
    None,
    "wasp"
  )
}