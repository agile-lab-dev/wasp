package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig

/**
  * Configuration model for ElasticSearch.
  *
  */
case class ElasticConfigModel(
                               connections: Seq[ConnectionConfig],
                               name: String
                             ) extends Model