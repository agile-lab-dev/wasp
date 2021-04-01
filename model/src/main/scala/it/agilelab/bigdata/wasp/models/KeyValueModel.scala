package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.KeyValueProduct
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}

import scala.util.Try

object KeyValueModel {

  val metadataAvro =
    s"""   {"namespace": "it.agilelab.wasp.avro",
		  |   "type": "record", "name": "metadata",
		  |    "fields": [
		  |        {"name": "id", "type": "string"},
			|        {"name": "sourceId", "type": "string"},
			|        {"name": "arrivalTimestamp", "type": "long"},
			|        {"name": "lastSeenTimestamp", "type": "long"},
			|        {"name": "path",
			|          "type": {
			|            "type": "array",
			|            "items": {
			|              "name": "Path",
			|              "type": "record",
			|              "fields": [
			|                {"name": "name", "type": "string"},
			|                {"name": "ts", "type": "long"}
			|              ]
			|            }
			|          }
			|        }
			|      ]
		  |  }""".stripMargin

  def metadataCatalog(cf: String) = s""" "metadata":{"cf":"$cf", "col":"m", "avro":"metadataAvroSchema"} """

  val metadataAvroSchemaKey = "metadataAvroSchema"

  def generateField(namespace: String, tableName: String, ownSchema: Option[String]): String = {
    val schema = (ownSchema :: Nil).flatten.mkString(", ")
    generate(namespace, tableName, schema)
  }

  def generateMetadataAndField(namespace: String, tableName: String, cf: String, ownSchema: Option[String]): String = {
    val schema = (Some(metadataCatalog(cf)) :: ownSchema :: Nil).flatten.mkString(", ")
    generate(namespace, tableName, schema)
  }

  private def generate(namespace: String, tableName: String, schema: String) = {
    s"""{
			 |"table":{"namespace":"$namespace", "name":"$tableName"},
			 |"rowkey":"key",
			 |"columns":{
			 |		$schema
			 |	}
			 |}""".stripMargin
  }

  def extractTableName(tableCatalog: String): Try[String] = {
    import spray.json._
    for {
      jsonMap <- Try(tableCatalog.parseJson).map(_.asJsObject.fields)
      tableMap <- Try(jsonMap.getOrElse("table", throw DeserializationException("Object 'table' not found"))).map(_.asJsObject.fields)
      namespace <- Try(tableMap.getOrElse("namespace", throw DeserializationException("Object 'table.namespace' not found"))).map {
        case JsString(v) => v
        case _ => throw DeserializationException("Value 'table.namespace' is not a string")
      }
      tableName <- Try(tableMap.getOrElse("name", throw DeserializationException("Object 'table.name' not found"))).map {
        case JsString(v) => v
        case _ => throw DeserializationException("Value 'table.name' is not a string")
      }
    } yield s"$namespace:$tableName"
  }

}

case class KeyValueModel(override val name: String,
                         tableCatalog: String,
                         dataFrameSchema: Option[String],
                         options: Option[Seq[KeyValueOption]],
                         useAvroSchemaManager: Boolean,
                         avroSchemas: Option[Map[String, String]])
  extends DatastoreModel {

  def getOptionsMap(): Map[String, String] = {
    options.map(sOpts => {
      sOpts.map(o => {
        (o.key, o.value)
      }).toMap
    }).getOrElse(Map())
  }

  override def datastoreProduct: DatastoreProduct = KeyValueProduct
}

case class KeyValueOption(key: String, value: String)