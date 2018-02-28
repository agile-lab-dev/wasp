package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig

case class ElasticConfigModel(connections: Seq[ConnectionConfig],
                              cluster_name: String,
                              name: String) extends Model