package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

object MetadataUtils {

  /** Retrieve an array of column with struct type expanse.
    */
  def flattenSchema(schema: StructType,
                    prefix: Option[String]): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName =
        if (prefix.isEmpty) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, Some(colName))
        case _              => Array(col(colName))
      }
    })
  }

}
