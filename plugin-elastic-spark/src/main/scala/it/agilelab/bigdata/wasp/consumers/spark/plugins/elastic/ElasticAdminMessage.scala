package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import it.agilelab.bigdata.wasp.core.WaspMessage
import it.agilelab.bigdata.wasp.core.models.configuration.ElasticConfigModel
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.sort.SortBuilder

trait ElasticAdminMessage extends WaspMessage

class AddressMessage(val host: String = ElasticAdminActor.host, val port: Int = ElasticAdminActor.port) extends ElasticAdminMessage

case class AddAlias(index: String = ElasticAdminActor.index, alias: String = ElasticAdminActor.alias) extends ElasticAdminMessage

case class AddIndex(index: String = ElasticAdminActor.index) extends ElasticAdminMessage

//case class AddNode(override val host : String = ElasticAdminActor.host, override val port : Int = ElasticAdminActor.port) extends AddressMessage(host, port)
case class AddMapping(index: String = ElasticAdminActor.index, datatype: String = ElasticAdminActor.dataType, schema: String = ElasticAdminActor.schema) extends ElasticAdminMessage

//case class CheckCluster() extends ElasticAdminMessage
case class CheckIndex(index: String = ElasticAdminActor.index) extends ElasticAdminMessage

//case class CheckNode(override val host : String = ElasticAdminActor.host, override val port : Int = ElasticAdminActor.port) extends AddressMessage(host, port)
case class RemoveAlias(index: String = ElasticAdminActor.index, alias: String = ElasticAdminActor.alias) extends ElasticAdminMessage

case class RemoveIndex(index: String = ElasticAdminActor.index) extends ElasticAdminMessage

case class RemoveMapping(index: String = ElasticAdminActor.index, datatype: String = ElasticAdminActor.dataType) extends ElasticAdminMessage

//case class RemoveNode(override val host : String = ElasticAdminActor.host, override val port : Int = ElasticAdminActor.port) extends AddressMessage(host, port)

case class Search(index: String, query: Option[QueryBuilder], sort: Option[SortBuilder[_]], from: Int, size: Int) extends ElasticAdminMessage

case class CheckOrCreateIndex(index: String = ElasticAdminActor.index, alias: String = ElasticAdminActor.alias, datatype: String = ElasticAdminActor.dataType, schema: String = ElasticAdminActor.schema) extends ElasticAdminMessage

case class Initialization(elasticConfigModel: ElasticConfigModel) extends ElasticAdminMessage