package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, ZookeeperConnection}
import org.mongodb.scala.bson.BsonObjectId


case class SolrConfigModel(
                            zookeeperConnections: ZookeeperConnection,
                            apiEndPoint: Option[ConnectionConfig],
                            name: String,
                            cluster_name: String
                          ) extends Model

