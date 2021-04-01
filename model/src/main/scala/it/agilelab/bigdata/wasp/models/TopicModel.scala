package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.KafkaProduct
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}
import org.bson.BsonDocument

object TopicModel {

  def name(basename: String) = s"${basename.toLowerCase}.topic"

  /**
    * Generate final schema for TopicModel. Use this method if you schema have a field metadata.
    *
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


sealed abstract class TopicCompression(val kafkaProp: String)

object TopicCompression {

  private[wasp] val _asString: Map[TopicCompression, String] = Map(
    TopicCompression.Disabled -> "disabled",
    TopicCompression.Gzip -> "gzip",
    TopicCompression.Snappy -> "snappy",
    TopicCompression.Lz4 -> "lz4"
  )

  def asString: PartialFunction[TopicCompression, String] = _asString

  def fromString: PartialFunction[String, TopicCompression] = _fromString

  private val _fromString: Map[String, TopicCompression] = _asString.map(_.swap)

  case object Disabled extends TopicCompression("none")

  case object Gzip extends TopicCompression("gzip")

  case object Snappy extends TopicCompression("snappy")

  case object Lz4 extends TopicCompression("lz4")

}

sealed trait SubjectStrategy

object SubjectStrategy {

  def subjectFor(schemaFQN: String, topicModel: TopicModel, isKey: Boolean) = {
    val suffix = if (isKey) {
      "-key"
    } else {
      "-value"
    }

    val subjectStrategy: Option[String] = topicModel.subjectStrategy match {
      case SubjectStrategy.None =>
        scala.None
      case SubjectStrategy.Topic =>
        Some(topicModel.name + suffix)
      case SubjectStrategy.Record =>
        Some(schemaFQN + suffix)
      case SubjectStrategy.TopicAndRecord =>
        Some(topicModel.name + "-" + schemaFQN + suffix)
    }
    subjectStrategy
  }

  private[wasp] val _asString: Map[SubjectStrategy, String] = Map(
    SubjectStrategy.None -> "none",
    SubjectStrategy.Topic -> "topic",
    SubjectStrategy.Record -> "record",
    SubjectStrategy.TopicAndRecord -> "topic-and-record"
  )

  def asString: PartialFunction[SubjectStrategy, String] = _asString

  def fromString: PartialFunction[String, SubjectStrategy] = _fromString

  private val _fromString: Map[String, SubjectStrategy] = _asString.map(_.swap)

  case object Topic extends SubjectStrategy

  case object Record extends SubjectStrategy

  case object TopicAndRecord extends SubjectStrategy

  case object None extends SubjectStrategy

}

/**
  * A model for a topic, that is, a message queue of some sort. Right now this means just Kafka topics.
  *
  * The `name` field specifies the name of the topic, and doubles as the unique identifier for the model in the
  * models database.
  *
  * The `creationTime` marks the time at which the model was generated.
  *
  * The `partitions` and `replicas` fields are used the configure the topic when the framework creates it.
  *
  * The `topicDataType` field specifies the format to use when encoding/decoding data to/from messages.
  *
  * The `keyFieldName` field allows you to optionally specify a field whose contents will be used as a message key when
  * writing to Kafka. The field must be of type string or binary. The original field will be left as-is, so you schema
  * must handle it (or you can use `valueFieldsNames`).
  *
  * The `headersFieldName` field allows you to optionally specify a field whose contents will be used as message headers
  * when writing to Kafka. The field must contain an array of non-null objects which  must have a non-null field
  * `headerKey` of type string and a field `headerValue` of type binary. The original field will be left as-is, so your
  * schema must handle it (or you can use `valueFieldsNames`).
  *
  * The `valueFieldsNames` field allows you to specify a list of field names to be used to filter the fields that get
  * passed to the value encoding; with this you can filter out fields that you don't need in the value, obviating the
  * need to handle them in the schema. This is especially useful when specifying the `keyFieldName` or
  * `headersFieldName`. For the avro and json topic data type this is optional; for the plaintext and binary topic data
  * types this field is mandatory and the list must contain a single value field name that has the proper type (string
  * for plaintext and binary for binary).
  *
  * The `schema` field contains the schema to use when encoding the value.
  */
case class TopicModel(override val name: String,
                      creationTime: Long,
                      partitions: Int,
                      replicas: Int,
                      topicDataType: String,
                      keyFieldName: Option[String],
                      headersFieldName: Option[String],
                      valueFieldsNames: Option[Seq[String]],
                      useAvroSchemaManager: Boolean,
                      schema: BsonDocument,
                      topicCompression: TopicCompression = TopicCompression.Disabled,
                      subjectStrategy: SubjectStrategy = SubjectStrategy.None,
                      keySchema: Option[String] = None
                     )
  extends DatastoreModel {
  def getJsonSchema: String = schema.toJson

  override def datastoreProduct: DatastoreProduct = KafkaProduct
}
object TopicDataTypes {
  val AVRO = "avro"
  val JSON = "json"
  val PLAINTEXT = "plaintext"
  val BINARY = "binary"
}