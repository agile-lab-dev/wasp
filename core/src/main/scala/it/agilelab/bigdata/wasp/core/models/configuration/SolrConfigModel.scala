package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, ZookeeperConnectionsConfig}

/**
  * Configuration model for Solr.
  *
  */
case class SolrConfigModel(
                            zookeeperConnections: ZookeeperConnectionsConfig,
                            name: String
                          ) extends Model