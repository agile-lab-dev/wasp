package it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.CdcModel
import org.apache.spark.sql.functions.{coalesce, col, max, struct}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

trait Writer {
  def write(df: DataFrame, id: Long): Unit
}

class DeltaLakeWriter(model: CdcModel, ss: SparkSession) extends Writer with Logging {

  def write(df: DataFrame, id: Long): Unit = {
    val keys: Array[String] = df.selectExpr("key.*").columns
    val path                = model.uri

    val afterImageDf = df.selectExpr(s"value.${GenericMutationFields.AFTER_IMAGE}.*")

    val deltaTable = DeltaLakeOperations.getDeltaTable(path, afterImageDf.schema, ss)

    val latestMutationDF = getLatestChangeForKey(df, keys).persist()

    val condition = keys.map(x => s"mutation.$x = table.$x").mkString(" AND ")

    try {
      val allColumnsButKeys = latestMutationDF.selectExpr("_value.*").drop(keys: _*).columns

      val deleteCondition = allColumnsButKeys.map(col => s"mutation.$col is null").mkString(" AND ")
      val insertCondition = allColumnsButKeys.map(col => s"mutation.$col is not null").mkString(" OR ")

      val mutationsDf = latestMutationDF.selectExpr(
        "key.*" +:
          allColumnsButKeys.map(eachCol => s"_value." + eachCol): _*
      )

      // schema evolution not supported yet, property not necessary at the moment
      // ss.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

      deltaTable
        .as("table")
        .merge(mutationsDf.as("mutation"), condition)
        .whenMatched(deleteCondition)
        .delete
        .whenMatched
        .updateAll
        .whenNotMatched(insertCondition)
        .insertAll
        .execute
    } finally {
      latestMutationDF.unpersist()
    }

  }

  def getLatestChangeForKey(df: DataFrame, keyFields: Seq[String]): DataFrame = {

    val keyCols: Seq[Column] = keyFields
      .map(keyField =>
        coalesce(
          col(s"latest.${GenericMutationFields.BEFORE_IMAGE}.$keyField"),
          col(s"latest.${GenericMutationFields.AFTER_IMAGE}.$keyField")
        ).as(keyField)
      )

    df.selectExpr(
        keyFields
          .map(x => s"key.$x") :+ s"struct(" +
          s"value.${GenericMutationFields.TIMESTAMP}, " +
          s"value.${GenericMutationFields.COMMIT_ID}, " +
          s"value.${GenericMutationFields.AFTER_IMAGE}, " +
          s"value.${GenericMutationFields.BEFORE_IMAGE}, " +
          s"value.${GenericMutationFields.TYPE}) as otherCols": _*
      )
      .groupBy(keyFields.map(col): _*)
      .agg(max("otherCols").as("latest"))
      .withColumn("key", struct(keyCols: _*))
      .selectExpr(
        Seq(
          s"latest.${GenericMutationFields.TIMESTAMP} as _timestamp",
          s"latest.${GenericMutationFields.COMMIT_ID} as _commitId",
          s"latest.${GenericMutationFields.TYPE} as _type",
          s"latest.${GenericMutationFields.AFTER_IMAGE} as _value",
          "key"
        ): _*
      )
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
    DeltaTable.forPath(path)
  }
}
