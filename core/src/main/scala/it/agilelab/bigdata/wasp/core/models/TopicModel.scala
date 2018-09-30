package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.utils.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType
import org.bson.BsonDocument

object TopicModel {

  def name(basename: String) = s"${basename.toLowerCase}.topic"

  /**
    * Generate final schema for TopicModel. Use this method if you schema have a field metadata.
    * @param ownSchema
    * @return
    */

  def generateField(namespace: String, name: String, ownSchema: Option[String]): String = {
    val schema = (ownSchema :: Nil).flatten.mkString(", ")
    generate(namespace, name, schema)
  }

  private def generate(namespace: String, name: String, schema: String) = {
    s"""
      {
        "type":"record",
        "namespace":"${namespace}",
        "name":"${name}",
        "fields":[
          ${schema}
        ]
      }
    """
  }
}

/**
  * A model for a topic, that is, a message queue of some sort. Right now this means just Kafka topics.
  *
  * @param name name of the topic
  * @param creationTime time at which the model was created
  * @param partitions number or partitions for the topic if/when the framework creates
  * @param replicas number or replicas for the topic if/when the framework creates
  * @param topicDataType format for encoding/decoding the data
  * @param keyFieldName optional name of the field to use as key for the messages when writing
  * @param headersFieldName optional name of the field to use as headers for the messages when writing
  * @param valueFieldsNames optional (sub)list of field names to be encoded in the message value when writing
  * @param schema schema for encoding/decoding the data
  */
case class TopicModel(override val name: String,
                      creationTime: Long,
                      partitions: Int,
                      replicas: Int,
                      topicDataType: String,
                      keyFieldName: Option[String],
                      headersFieldName: Option[String],
                      valueFieldsNames: Option[Seq[String]],
                      schema: BsonDocument)
    extends DatastoreModel[TopicCategory] {
  def getJsonSchema: String = schema.toJson
  def getDataType: DataType = {
    val schemaAvro = new Schema.Parser().parse(this.getJsonSchema)
    SchemaConverters.toSqlType(schemaAvro).dataType
  }
}