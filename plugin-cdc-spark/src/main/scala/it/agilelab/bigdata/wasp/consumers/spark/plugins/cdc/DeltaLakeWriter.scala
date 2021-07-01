package it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{CdcModel, GenericCdcMutationFields}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, max, struct}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

trait Writer {
  def write(df: DataFrame, id: Long): Unit
}

class DeltaLakeWriter(model: CdcModel, ss: SparkSession) extends Writer with Logging {

  def write(df: DataFrame, id: Long): Unit = {
    val keys: Array[String] = df.selectExpr("key.*").columns
    val path = model.uri

    val afterImageDf = df.selectExpr(s"value.${GenericCdcMutationFields.AFTER_IMAGE}.*")

    val deltaTable = DeltaLakeOperations.getDeltaTable(path, afterImageDf.schema, ss)

    val latestMutationDF = getLatestChangeForKey(df, keys).persist()

    try {
      val condition = keys.map(x => s"mutation.$x = table.$x").mkString(" AND ")

      val allColumnsButKeys = latestMutationDF.selectExpr("_value.*").drop(keys: _*).columns

      val deleteCondition = allColumnsButKeys.map(col => s"mutation.$col is null").mkString(" AND ")
      val insertCondition = allColumnsButKeys.map(col => s"mutation.$col is not null").mkString(" OR ")

      val mutationsDf = latestMutationDF.selectExpr(
        "key.*" +:
          allColumnsButKeys.map(eachCol => s"_value." + eachCol): _*
      )

      // schema evolution not supported yet, property not necessary at the moment
      // ss.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

      /*
      TODO: check if the following condition can happen:
      If in the same minibatch you get first the update
      and then the insert of the same record, by doing so
      you miss the update modification, or am I wrong?
      Because in that case the update will not be found,
      but then the insert will be found and,
      consequently,
      in the whenNotMatched you will insert an outdated data.
       */

      deltaTable
        .as("table")
        // map each mutation to the corresponding row of the delta table
        .merge(mutationsDf.as("mutation"), condition)
        // if all fields are null in the mutation (the incoming mutation) dataframe
        // that means that the row is deleted
        .whenMatched(deleteCondition)
        .delete()
        // if the primary key match the condition is reached and
        // the row is not deleted (because otherwise the actions are discarted)
        // update the fields
        .whenMatched()
        .updateAll()
        // if the primary key doesn't match any row, then create the row
        .whenNotMatched(insertCondition)
        .insertAll()
        .execute()
    } finally {
      latestMutationDF.unpersist()
    }

  }

  def getLatestChangeForKey(df: DataFrame, keyFields: Seq[String]): DataFrame = {

    val keyCols: Seq[Column] = keyFields
      .map(keyField =>
        coalesce(
          col(s"latest.${GenericCdcMutationFields.BEFORE_IMAGE}.$keyField"),
          col(s"latest.${GenericCdcMutationFields.AFTER_IMAGE}.$keyField")
        ).as(keyField)
      )

    df.selectExpr(
      keyFields
        .map(x => s"key.$x") :+ s"struct(" +
        s"value.${GenericCdcMutationFields.TIMESTAMP}, " +
        s"value.${GenericCdcMutationFields.COMMIT_ID}, " +
        s"value.${GenericCdcMutationFields.AFTER_IMAGE}, " +
        s"value.${GenericCdcMutationFields.BEFORE_IMAGE}, " +
        s"value.${GenericCdcMutationFields.TYPE}) as otherCols": _*
    )
      .groupBy(keyFields.map(col): _*)
      .agg(max("otherCols").as("latest"))
      .withColumn("key", struct(keyCols: _*))
      .selectExpr(
        Seq(
          s"latest.${GenericCdcMutationFields.TIMESTAMP} as _timestamp",
          s"latest.${GenericCdcMutationFields.COMMIT_ID} as _commitId",
          s"latest.${GenericCdcMutationFields.TYPE} as _type",
          s"latest.${GenericCdcMutationFields.AFTER_IMAGE} as _value",
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
