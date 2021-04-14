package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model.ContinuousUpdateModel
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.hadoop.hive.ql.metadata.Hive.SchemaException
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, first}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

trait Writer {
  def write(path: String, df: DataFrame): Unit
}

class DeltaLakeWriter(parallelWriteModel: ContinuousUpdateModel, ss: SparkSession) extends Writer with Logging {

  def getStructFieldFromColumn(col: Column): StructField = StructField(col.name, CatalystSqlParser.parseDataType(col.dataType))

  def write(path: String, df: DataFrame): Unit = {

    val keys: List[String] = parallelWriteModel.keys
    val orderingExpression = parallelWriteModel.orderingExpression
    val fieldsToDrop = parallelWriteModel.fieldsToDrop

    //TODO check how to retrieve domain.entity_version properly
    val catalogSchema = new StructType(ss.catalog.listColumns(parallelWriteModel.tableName).collect().map(getStructFieldFromColumn))
    val deltaTable = DeltaLakeOperations.getDeltaTable(path, catalogSchema, ss)
    if (df.drop(fieldsToDrop: _*).schema != catalogSchema) throw new SchemaException(s"Different schema detected, expected $catalogSchema, found ${df.drop(fieldsToDrop:_*).schema}" )

    val condition: String = keys.map(x => s"table.$x = table2.$x").mkString(" AND ")

    val orderingColName: String = "_orderWaspDeltaLake_"
    val dfWithOrderingColumn = df.select(df.columns.map(col): _*).withColumn(orderingColName, expr(orderingExpression))
    val windowSpec = Window.partitionBy(keys.map(col): _*).orderBy(col(orderingColName).desc)

    val myDF: DataFrame = dfWithOrderingColumn.distinct()
      .withColumn("max_"+orderingColName, first(col(orderingColName)).over(windowSpec)
        .as("max_"+orderingColName))
        .filter(orderingColName + " = max_"+orderingColName)
        .drop("max_"+orderingColName)
      .drop(orderingColName)
      .drop(fieldsToDrop: _*)
    try {

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
}

object DeltaLakeOperations extends Logging {

  def getDeltaTable(path: String, schema: StructType, ss: SparkSession): DeltaTable = {
    Try(DeltaTable.forPath(path)) match {
      case Failure(_) =>
        logger.info("Creating a new empty Delta table.")
        initDeltaTable(path, schema, ss)
      case Success(value) => value
    }
  }

  def initDeltaTable(path: String, schema: StructType, ss: SparkSession): DeltaTable = {
    val emptyDF = ss.createDataFrame(ss.sparkContext.emptyRDD[Row], schema)
    emptyDF.write.format("delta").save(path)
    DeltaTable.forPath(ss, path)
  }
}
