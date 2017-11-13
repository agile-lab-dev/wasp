package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.BsonObjectId

// TODO external scaladocs links
/**
	*
	*/
case class KeyValueModel(override val name: String,
                         tableCatalog: String,
                         dataFrameSchema: Option[String],
                         options: Option[Seq[KeyValueOption]],
                         avroSchemas: Option[Map[String, String]],
                         _id: Option[BsonObjectId] = None) extends Model {
	def getOptionsMap(): Map[String, String] = {
		options.map(sOpts => {
			sOpts.map(o => {
				(o.key, o.value)
			}).toMap
		}).getOrElse(Map())
	}
}

object KeyValueModel {

	/*
	val metadataAvro = s"""   {"namespace": "it.agilelab.wasp.avro",
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


	def metdataCatalog(cf: String) = s""" "metadata":{"cf":"$cf", "col":"m", "avro":"metadataAvroSchema"} """
	val metadataAvroSchemaKey = "metadataAvroSchema"
*/
	def generateField(namespace: String, tableName: String, ownSchema: Option[String]): String = {
		val schema = (ownSchema :: Nil).flatten.mkString(", ")
		generate(namespace, tableName, schema)
	}

	/*
	def generateMetadataAndField(namespace: String, tableName: String, cf: String, ownSchema: Option[String]): String = {
		val schema = (Some(metdataCatalog(cf))  :: ownSchema :: Nil).flatten.mkString(", ")
		generate(namespace, tableName, schema)
	}
*/
	private def generate(namespace: String, tableName: String, schema: String) = {
		s"""{
			 |"table":{"namespace":"$namespace", "name":"$tableName"},
			 |"rowkey":"key",
			 |"columns":{
			 |		$schema
			 |	}
			 |}""".stripMargin
	}

}

case class KeyValueOption(key: String, value: String)