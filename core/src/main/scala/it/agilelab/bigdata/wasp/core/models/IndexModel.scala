package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.utils.ConfigManager
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

  val metadata_solr = """
    {"name": "metadata.id", "type": "string", "stored":true },
    {"name": "metadata.sourceId", "type": "string", "stored":true },
    {"name": "metadata.arrivalTimestamp", "type": "tlong", "stored":true},
    {"name": "metadata.lastSeenTimestamp", "type": "tlong", "stored":true},
    {"name": "metadata.path", "type": "string", "stored":"true", "multiValued":"true"}
  """

  def normalizeName(basename: String) = s"${basename.toLowerCase}_index"

  def generateField(indexType: IndexType.Type, name: Option[String], ownSchema: Option[String]): String = {

    indexType match {
      case IndexType.ELASTIC =>
        val schema = (ownSchema :: Nil).flatten.mkString(", ")
        generateElastic(name, schema)

      case IndexType.SOLR =>
        val schema = (ownSchema :: Nil).flatten.mkString(", ")
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
        val schema = (Some(metadata_elastic) :: ownSchema :: Nil).flatten.mkString(", ")
        generateElastic(name, schema)

      case IndexType.SOLR =>
        val schema = (Some(metadata_solr) :: ownSchema :: Nil).flatten.mkString(", ")
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
                      query: Option[String] = None,
                      numShards: Option[Int] = Some(1),
                      replicationFactor: Option[Int] = Some(1),
                      rollingIndex: Boolean = true,
                      idField: Option[String] = None)
  extends Model {

  def resource = s"$eventuallyTimedName/$dataType"

  def collection = eventuallyTimedName

  def eventuallyTimedName = if (rollingIndex) ConfigManager.buildTimedName(name) else name
  
  /**
    * Returns a JSON representation of the schema of this index's schema.
    * @return
    */
  def getJsonSchema: String = {
    // TODO check: this if not allow to correctly work with Elastic (see GL-69)
    if (ConfigManager.getWaspConfig.defaultIndexedDatastore == Datastores.solrProduct) {
      val solrProperties = this
        .schema
        .get
        .get("properties")
        .asArray()
        .getValues
        .asScala
        .map(e => e.asDocument().toJson)
        .mkString(",")

      s"[${solrProperties}]"
    } else {
      //ConfigManager.getWaspConfig.defaultIndexedDatastore == Datastores.elasticProduct
      schema.getOrElse(new BsonDocument).toJson
    }
  }

  def dataType: String = name
}