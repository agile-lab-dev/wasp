package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct


/**
  * A named model for mutations coming from a CDC tool. This model should be used together with the Cdc writer plugin
  * in order to write these mutations into a Delta Lake table on HDFS.
  *
  * `uri` is the location on HDFS where the Delta Table will be created.
  *
  * `schema` is a json-encoded DataFrame schema, that is, a StructType. See DataType.fromJson and DataType.json.
  *
  * `options` control the underlying spark DeltaLakeWriter in the writers using an instance of this model.
  *
  * @param name    the name of the datastore
  * @param uri     the uri where the data are meant to be written
  * @param schema  the schema of the data
  * @param options the options for the datastore
  */

case class CdcModel(override val name: String,
                    uri: String,
                    schema: String,
                    options: CdcOptions = CdcOptions.default)
  extends DatastoreModel {
  override def datastoreProduct: DatastoreProduct = DatastoreProduct.CdcProduct
}

/**
  * Options for a CdcModel:
  *
  * `saveMode` specifies the behaviour when saving and the output uri already exists; valid values are:
  *   - "error", throw an error and do not save anything
  *   - "overwrite", overwrite existing data
  *   - "append", append to existing data
  *   - "ignore", do not save anything and don't throw any errors
  *   - "default", like "error" for it.agilelab.bigdata.wasp.consumers.SparkWriter, like "append" for it.agilelab.bigdata.wasp.consumers.SparkStreamingWriter
  *
  * `format` specifies the data format to use; valid values are:
  *  - "delta" (this is the default)
  *  - "parquet"
  *  - "orc"
  *  - "json"
  *  - any format accepted by the available Spark DataFrameWriters
  *
  * `extraOptions` allows specifying any writer-specific options accepted by DataFrameReader/Writer.option
  *
  * `partitionBy` allows specifying columns to be used to partition the data by using different directories for
  * different values
  *
  * @param saveMode     specifies the behaviour when the output uri exists
  * @param format       specifies the format to use
  * @param extraOptions extra options for the underlying writer
  */
case class CdcOptions(saveMode: String,
                      format: String = "delta",
                      extraOptions: Option[Map[String, String]] = None,
                      partitionBy: Option[List[String]] = None)

object CdcOptions {
  lazy val default = CdcOptions("default")
  lazy val defaultAppend = CdcOptions("append")
}