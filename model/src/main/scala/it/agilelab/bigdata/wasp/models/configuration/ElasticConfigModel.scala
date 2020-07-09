package it.agilelab.bigdata.wasp.models.configuration

import it.agilelab.bigdata.wasp.models.Model

/**
  * Configuration model for ElasticSearch.
  *
  */
case class ElasticConfigModel(
                               connections: Seq[ConnectionConfig],
                               name: String
                             ) extends Model