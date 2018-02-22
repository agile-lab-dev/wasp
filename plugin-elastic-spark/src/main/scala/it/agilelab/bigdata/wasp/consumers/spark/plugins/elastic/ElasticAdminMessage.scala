package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import it.agilelab.bigdata.wasp.core.WaspMessage
import it.agilelab.bigdata.wasp.core.models.configuration.ElasticConfigModel

sealed abstract class ElasticAdminMessage extends WaspMessage

case class AddAlias(index: String, alias: String ) extends ElasticAdminMessage

case class AddIndex(index: String) extends ElasticAdminMessage

case class AddMapping(index: String, datatype: String, schema: String ) extends ElasticAdminMessage

case class CheckIndex(index: String) extends ElasticAdminMessage

case class RemoveAlias(index: String, alias: String) extends ElasticAdminMessage

case class RemoveIndex(index: String) extends ElasticAdminMessage

case class RemoveMapping(index: String, datatype: String) extends ElasticAdminMessage

case class CheckOrCreateIndex(index: String, alias: String, datatype: String, schema: String) extends ElasticAdminMessage

case class Initialization(elasticConfigModel: ElasticConfigModel) extends ElasticAdminMessage