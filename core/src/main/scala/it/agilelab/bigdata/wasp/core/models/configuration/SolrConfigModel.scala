package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, ZookeeperConnection}

/**
  * Configuration model for Solr.
  *
  */
case class SolrConfigModel(
                            zookeeperConnections: ZookeeperConnection,
                            apiEndPoint: Option[ConnectionConfig],
                            name: String
                          ) extends Model