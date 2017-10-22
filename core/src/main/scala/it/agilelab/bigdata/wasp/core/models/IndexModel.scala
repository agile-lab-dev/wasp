package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, JsonConverter}
import kafka.utils.Json
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

import scala.collection.JavaConverters._

object IndexModel {
  val readerType = "index"

  val metadata_elastic = """
        "id":{"type": "string","index":"not_analyzed","store":"true"},
        "sourceId":{"type": "string","index":"not_analyzed","store":"true"},
        "arrivalTimestamp": {"type": "long", "index":"not_analyzed","store":"true"},
        "lastSeenTimestamp": { "type": "long", "index":"not_analyzed","store":"true"},
        "path": { "type": "nested"}
  """

  val schema_base_elastic = """
    "id_event":{"type":"double","index":"not_analyzed","store":"true"},
    "source_name":{"type":"string","index":"not_analyzed","store":"true"},
    "Index_name":{"type":"string","index":"not_analyzed","store":"true"},
    "metric_name":{"type":"string","index":"not_analyzed","store":"true"},
    "timestamp":{"type":"date","format":"date_time","index":"not_analyzed","store":"true"},
    "latitude":{"type":"double","index":"not_analyzed","store":"true"},
    "longitude":{"type":"double","index":"not_analyzed","store":"true"},
    "value":{"type":"double","index":"not_analyzed","store":"true"},
    "payload":{"type":"string","index":"not_analyzed","store":"true"}
  """

  val metadata_solr = """
    {"name": "id", "type": "string", "stored":true },
    {"name": "sourceId", "type": "string", "stored":true },
    {"name": "arrivalTimestamp", "type": "tlong", "stored":true},
    {"name": "lastSeenTimestamp", "type": "tlong", "stored":true},
    {"name": "path", "type": "string", "store":"true", "multiValued":"true"}
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

  def generateField(indexType: IndexType.Type, name: Option[String], ownSchema: Option[String]): String = {

    indexType match {
      case IndexType.ELASTIC =>
        val schema = (Some(IndexModel.schema_base_elastic) :: ownSchema :: Nil).flatten.mkString(", ")
        generateElastic(name, schema)

      case IndexType.SOLR =>
        val schema = (Some(IndexModel.schema_base_solr) :: ownSchema :: Nil).flatten.mkString(", ")
        generateSolr(schema)
    }

  }

  /**
    * Generate final schema for TopicModel. Use this method if you schema not have a field metadata.
    * @param ownSchema
    * @return
    */

  def generateMetadataAndField(indexType: IndexType.Type, name: Option[String], ownSchema: Option[String]): String = {
    indexType match {
      case IndexType.ELASTIC =>
        val schema = (Some(metadata_elastic) :: Some(IndexModel.schema_base_elastic) :: ownSchema :: Nil).flatten.mkString(", ")
        generateElastic(name, schema)

      case IndexType.SOLR =>
        val schema = (Some(metadata_solr) :: Some(IndexModel.schema_base_solr) :: ownSchema :: Nil).flatten.mkString(", ")
        generateSolr(schema)
    }
  }

  private def generateElastic(name: Option[String], schema: String) = {
    s"""{
      "${name.getOrElse("defaultElastic")}":{
        "properties":{
          ${schema}
        }
      }}"""
  }

  private def generateSolr(schema: String) = {
    s"""{
      "properties": [
        ${schema}
      ]
    }"""
  }

}

case class IndexModel(override val name: String,
                      creationTime: Long,
                      schema: Option[BsonDocument],
                      _id: Option[BsonObjectId] = None,
                      query: Option[String] = None,
                      numShards: Option[Int] = Some(1),
                      replicationFactor: Option[Int] = Some(1)) extends Model {

  def resource = s"${ConfigManager.buildTimedName(name)}/$dataType"

  def collection = s"${ConfigManager.buildTimedName(name)}"
  
  /**
    * Returns a JSON representation of the schema of this index's schema.
    * @return
    */
  def getJsonSchema: String = schema.getOrElse(new BsonDocument).toJson
  
  /**
    * Returns the data type of the contents of this index.
    */
  def dataType: String = {
    schema
      .map(bson => {
        bson
          .keySet() // grab keys; order should be right as it is backed by a LinkedHashMap
          .asScala
          .headOption
          .getOrElse("undefined") // TODO default in case the schema is wrong? does it even make sense?
      })
      .getOrElse("undefined") // TODO default in case the schema is absent? does it even make sense?
  }

}
