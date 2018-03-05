package it.agilelab.bigdata.wasp.core.models

object RawModel{
	val metadata = """
    {"name": "id", "type":"string", "nullable":false, "metadata":{}},
    {"name": "sourceId", "type":"string", "nullable":false, "metadata":{}},
    {"name": "arrivalTimestamp", "type":"long", "nullable":false, "metadata":{}},
    {"name": "lastSeenTimestamp", "type":"long", "nullable":false, "metadata":{}},
    {"name": "path", "type":"string", "nullable":false, "metadata":{}}
  """

	/**
		* Generate final schema for RawModel. Use this method if you schema have a field metadata.
		* @param ownSchema
		* @return
		*/

	def generateField(ownSchema: Option[String]): String = {
		val schema = (ownSchema :: Nil).flatten.mkString(", ")
		generate(schema)
	}

	/**
		* Generate final schema for RawModel. Use this method if you schema not have a field metadata.
		* @param ownSchema
		* @return
		*/

	def generateMetadataAndField(ownSchema: Option[String]): String = {
		val schema = (Some(metadata)  :: ownSchema :: Nil).flatten.mkString(", ")
		generate(schema)
	}

	private def generate(schema: String) = {
		s"""
			 |{
			 |      "type":"struct",
			 |      "fields":[
			 |        ${schema}
			 |      ]
			 |}
       """.stripMargin
	}
}

// TODO external scaladocs links
/**
	* A named model for data stored as files on a raw datastore (eg HDFS).
	*
	* The `uri` is augmented with time information if `timed` is true. For writers this means whether to use `uri`
	* as-is or create timed namespaces (eg for HDFS, a subdirectory) inside; for readers whether to read from `uri` as-is
	* or from the most recent timed namespace inside.
	*
	* `schema` is a json-encoded DataFrame schema, that is, a StructType. See DataType.fromJson and DataType.json.
	*
	* `options` control the underlying spark DataFrameWriter/Reader in the writers/readers using an instance of this model.
	*
	* @param name the name of the datastore
	* @param uri the uri where the data files reside
	* @param timed whether the uri must be augmented with time information
	* @param schema the schema of the data
	* @param options the options for the datastore
	*/

case class RawModel(override val name: String,
                    uri: String,
                    timed: Boolean = true,
                    schema: String,
                    options: RawOptions = RawOptions.default) extends Model

// TODO external scaladocs links
/**
	* Options for a raw datastore.
	*
	* `saveMode` specifies the behaviour when saving and the output uri already exists; valid values are:
	*   - "error", throw an error and do not save anything
	*   - "overwrite", overwrite existing data
	*   - "append", append to existing data
	*   - "ignore", do not save anything and don't throw any errors
	*   - "default", like "error" for it.agilelab.bigdata.wasp.consumers.SparkWriter, like "append" for it.agilelab.bigdata.wasp.consumers.SparkStreamingWriter
	*
	* `format` specifies the data format to use; valid values are:
	*  - "parquet" (this is the default)
	*  - "orc"
	*  - "json"
	*  - any format accepted by the available Spark DataFrameWriters
	*
	*  `extraOptions` allows specifying any writer-specific options accepted by DataFrameReader/Writer.option
	*
	*  `partitionBy` allows specifying columns to be used to partition the data by using different directories for
	*  different values
	*
	* @param saveMode specifies the behaviour when the output uri exists
	* @param format specifies the format to use
	* @param extraOptions extra options for the underlying writer
	*/
case class RawOptions(saveMode: String,
                      format: String,
                      extraOptions: Option[Map[String, String]] = None,
                      partitionBy: Option[List[String]] = None)

object RawOptions {
	lazy val default = RawOptions("default", "parquet")
	lazy val defaultAppend = RawOptions("append", "parquet")
}