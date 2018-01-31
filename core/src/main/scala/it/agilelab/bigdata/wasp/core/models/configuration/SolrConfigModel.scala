package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, ZookeeperConnection}
import org.mongodb.scala.bson.BsonObjectId


case class SolrConfigModel(
                            zookeeperConnections: ZookeeperConnection,
                            apiEndPoint: Option[ConnectionConfig],
                            name: String,
                            _id: Option[BsonObjectId],
                            cluster_name: String
                          )

