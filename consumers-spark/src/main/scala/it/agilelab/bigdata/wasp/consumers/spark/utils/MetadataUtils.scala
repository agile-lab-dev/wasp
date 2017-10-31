package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

object MetadataUtils {

  /** Retrieve an array of column with struct type expanse.
    */
  def flatMetadataSchema(schema: StructType,
                         prefix: Option[String]): Array[Column] = {
    schema.fields.flatMap(f => {
      if (f.name == "metadata" || prefix == "metadata") {
        val colName =
          if (prefix.isEmpty) f.name else (prefix.getOrElse("") + "." + f.name)

        f.dataType match {
          case st: StructType => flatMetadataSchema(st, Some(colName))
          case _ => Array(col(colName).alias(colName))
        }
      } else {
        Array(col(f.name))
      }
    })
  }

}
