package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.ContinuousUpdate
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{HadoopS3Utils, HadoopS3aUtil}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, first}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.{Failure, Success, Try}

class SchemaException(message: String) extends Exception(message)

case class ContinuousUpdateWriter(writerDetails: ContinuousUpdate) extends ParallelWriter {

  def getStructFieldFromColumn(col: Column): StructField =
    StructField(col.name, CatalystSqlParser.parseDataType(col.dataType))

  def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit = {
    if (new HadoopS3aUtil(df.sparkSession.sparkContext.hadoopConfiguration, writeExecutionPlan.temporaryCredentials.w).performBulkHadoopCfgSetup.isFailure)
      throw new Exception("Failed Hadoop settings configuration")

    val s3path: String = HadoopS3Utils.useS3aScheme(writeExecutionPlan.writeUri).toString()
    write(writerDetails, s3path, df)
  }

  private def write(continuousUpdateFlavour: ContinuousUpdate, s3path: String, df: DataFrame) = {

    val catalogSchema = new StructType(
      df.sparkSession.catalog.listColumns(continuousUpdateFlavour.tableName).collect().map(getStructFieldFromColumn)
    )
    val deltaTable = DeltaLakeOperations.getDeltaTable(s3path)
    if (df.drop(continuousUpdateFlavour.fieldsToDrop: _*).schema != catalogSchema)
      throw new SchemaException(
        s"Different schema detected, expected $catalogSchema, found ${df.drop(continuousUpdateFlavour.fieldsToDrop: _*).schema}"
      )

    val condition: String = continuousUpdateFlavour.keys.map(x => s"table.$x = table2.$x").mkString(" AND ")

    val orderingColName: String = "_orderWaspDeltaLake_"
    val dfWithOrderingColumn =
      df.select(df.columns.map(col): _*).withColumn(orderingColName, expr(continuousUpdateFlavour.orderingExpression))
    val windowSpec = Window.partitionBy(continuousUpdateFlavour.keys.map(col): _*).orderBy(col(orderingColName).desc)

    val myDF: DataFrame = dfWithOrderingColumn
      .distinct()
      .withColumn(
        "max_" + orderingColName,
        first(col(orderingColName))
          .over(windowSpec)
          .as("max_" + orderingColName)
      )
      .filter(orderingColName + " = max_" + orderingColName)
      .drop("max_" + orderingColName)
      .drop(orderingColName)
      .drop(continuousUpdateFlavour.fieldsToDrop: _*)

    // schema evolution not supported yet, property not necessary at the moment
    // ss.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    deltaTable
      .as("table")
      .merge(myDF.as("table2"), condition)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll
      .execute
  }
}

object DeltaLakeOperations extends Logging {

  def getDeltaTable(path: String): DeltaTable = {
    Try(DeltaTable.forPath(path)) match {
      case Failure(_)     => throw new Exception(s"$path does not refer to a delta table")
      case Success(value) => value
    }
  }

}
