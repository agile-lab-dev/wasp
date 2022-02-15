package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ContinuousUpdate
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{DataCatalogService, GlueDataCatalogService}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, first}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI

class SchemaException(message: String) extends Exception(message)

/**
  * Writer for continuous update.
  * @param writerDetails  Informations about unique keys, ordering expression and fields to drop
  * @param entityDetails
  */
case class ContinuousUpdateWriter(writerDetails: ContinuousUpdate, entityDetails: CatalogCoordinates, catalogService: DataCatalogService) extends DeltaParallelWriterTrait {

  override def performDeltaWrite(df: DataFrame, s3path: URI, partitioningColumns: Seq[String]): Unit = {
    // schema evolution not supported yet, property not necessary at the moment
    // ss.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    val spark: SparkSession = df.sparkSession
    val schema: StructType = catalogService.getSchema(spark, entityDetails)
    val orderedDF = applyOrderingLogic(checkSchema(df, schema), writerDetails.keys, writerDetails.orderingExpression)
    val condition = writerDetails.keys.map(x => s"table.$x = table2.$x").mkString(" AND ")
    val deltaTable = getDeltaTable(s3path, spark, partitioningColumns)
    val enforcedDf = enforceSchema(orderedDF, schema)
    deltaTable
      .as("table")
      .merge(enforcedDf.as("table2"), condition)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  private def enforceSchema(df: DataFrame, schema: StructType): DataFrame = {
    df.selectExpr(schema.map(_.name): _*)
  }

  private def applyOrderingLogic(df: DataFrame,
                                 keys: List[String],
                                 orderingExpression: String): DataFrame = {

    val orderingColName: String = "_orderWaspDeltaLake_"
    val dfWithOrderingColumn = df.select(df.columns.map(col): _*).withColumn(orderingColName, expr(orderingExpression))
    val windowSpec = Window.partitionBy(keys.map(col): _*).orderBy(col(orderingColName).desc)

    dfWithOrderingColumn.distinct()
      .withColumn("max_" + orderingColName, first(col(orderingColName)).over(windowSpec)
        .as("max_" + orderingColName))
      .filter(orderingColName + " = max_" + orderingColName)
      .drop("max_" + orderingColName)
      .drop(orderingColName)
  }

  private def checkSchema(df: DataFrame,
                          catalogSchema: StructType): DataFrame = {
    val dfFields = df.schema.fields.map(field => field.copy(name = field.name.toLowerCase))
    if (dfFields.intersect(catalogSchema.fields.map(field => field.copy(name = field.name.toLowerCase))).length != catalogSchema.fields.length)
      throw new SchemaException(
        s"""
        Different schema detected\n
        expected a subset of fields: ${catalogSchema.fields.mkString("Array(", ", ", ")")}, \n
        found: ${dfFields.mkString("Array(", ", ", ")")}""")
    df
  }
}