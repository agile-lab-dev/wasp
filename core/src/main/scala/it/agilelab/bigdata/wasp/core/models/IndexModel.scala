package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import reactivemongo.bson.{BSONDocument, BSONObjectID, BSONString}

object IndexModel {
  val readerType = "index"

  val schema_base_elastic =
    """
    "id_event":{"type":"double","index":"not_analyzed","store":"true","enabled":"true"},
    "source_name":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
    "Index_name":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
    "metric_name":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
    "timestamp":{"type":"date","format":"date_time","index":"not_analyzed","store":"true","enabled":"true"},
    "latitude":{"type":"double","index":"not_analyzed","store":"true","enabled":"true"},
    "longitude":{"type":"double","index":"not_analyzed","store":"true","enabled":"true"},
    "value":{"type":"double","index":"not_analyzed","store":"true","enabled":"true"},
    "payload":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"}
               """

  val schema_base_solr = """
                         { "name":"id_event", "type":"tdouble", "stored":true },
                         { "name":"source_name", "type":"string", "stored":true },
                         { "name":"topic_name", "type":"string","stored":true },
                         { "name":"metric_name", "type":"string","stored":true },
                         { "name":"timestamp", "type":"string","stored":true },
                         { "name":"latitude", "type":"tdouble", "stored":true },
                         { "name":"longitude", "type":"tdouble", "stored":true },
                         { "name":"value", "type":"string", "stored":true },
                         { "name":"payload", "type":"string", "stored":true }
                    """

  def normalizeName(basename: String) = s"${basename.toLowerCase}_index"
}

case class IndexModel(override val name: String,
                      creationTime: Long,
                      schema: Option[BSONDocument],
                      _id: Option[BSONObjectID] = None,
                      query: Option[String] = None,
                      numShards: Option[Int] = Some(1),
                      replicationFactor: Option[Int] = Some(1))
    extends Model {

  def resource = s"${ConfigManager.buildTimedName(name)}/$dataType"

  def collection = s"${ConfigManager.buildTimedName(name)}"

  def dataType =
    schema
      .map(
          bson =>
            bson.elements.headOption
              .getOrElse(("undefined", BSONString("undefined")))
              ._1)
      .getOrElse("undefined")

}
