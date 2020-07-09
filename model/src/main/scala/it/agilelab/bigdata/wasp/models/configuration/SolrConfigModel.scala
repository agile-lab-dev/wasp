package it.agilelab.bigdata.wasp.models.configuration

import it.agilelab.bigdata.wasp.models.Model

/**
  * Configuration model for Solr.
  *
  */
case class SolrConfigModel(
                            zookeeperConnections: ZookeeperConnectionsConfig,
                            name: String
                          ) extends Model