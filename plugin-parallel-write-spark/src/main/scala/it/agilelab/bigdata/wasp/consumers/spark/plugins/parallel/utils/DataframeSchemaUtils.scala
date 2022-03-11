package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import scala.util.Try

object DataframeSchemaUtils {

  /**
   * Manipulates a dataframe to be compliant with a schema, first selects the dataframe
   * than checks that the selected schema is compliant with the provided schema
   * @param df Dataframe to manipulate
   * @param schema schema that must be enforced
   * @return Either a converted dataframe or a throwable
   */
  def convertToSchema(df: DataFrame, schema: StructType): Try[DataFrame] =
    for {
      _          <- SchemaChecker.isSelectable(schema, df.schema)
      selectedDf = df.select(schema.map(_.name).map(col): _*)
      _          <- SchemaChecker.isValid(schema, selectedDf.schema)
    } yield selectedDf
}
