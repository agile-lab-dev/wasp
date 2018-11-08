package org.apache.hadoop.hbase.spark

import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.datasources.hbase.{Field, HBaseTableCatalog, Utils}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

/**
  * Created by Agile Lab s.r.l. on 04/11/2017.
  */
class HBaseSink(sparkSession: SparkSession, parameters: Map[String, String], hBaseContext: HBaseContext) extends Sink with Logging {

  /**
    * Non è gestito il commit log quindi un task può essere inserito due volte
    * Per creare questo metodo ho seguito il codice della libreria
    * elasticsearch-spark-20_2.11-6.0.0-rc1-sources.jar -> org.elasticsearch.spark.sql.streaming.EsSparkSqlStreamingSink
    *
    * @param batchId
    * @param data
    */
  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    val queryExecution: QueryExecution = data.queryExecution
    val schema: StructType = data.schema
    val putConverterFactory = PutConverterFactory(parameters, schema)
    val convertToPut: InternalRow => Put = putConverterFactory.convertToPut
    val hBaseContextInternal = hBaseContext
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      hBaseContextInternal
        .bulkPut(queryExecution.toRdd,
          putConverterFactory.getTableName(),
          convertToPut
        )
    }
  }

  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logWarning(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }
}

case class PutConverterFactory(@transient parameters: Map[String, String],
                               @transient schema: StructType) {
  @transient val catalog = HBaseTableCatalog(parameters)

  @transient val rkFields: Seq[Field] = catalog.getRowKey
  val rkIdxedFields: Seq[(Int, Field)] = rkFields.map { case x =>
    (schema.fieldIndex(x.colName), x)
  }

  assert(rkIdxedFields.nonEmpty,s"""
                                  |You should put at least one column for the key of hbase row in the tableCatalog of the KeyValueModel. ex:
                                  |"id":{"cf":"rowkey", "col":"key", "type":"string"}
                                  |""".stripMargin
  )

  val useSchemaAvroManager: Boolean = catalog.get("useAvroSchemaManager").map(_.toBoolean).getOrElse(false)

  val darwinConf = if (parameters.get("useavroschemamanager").map(_.toBoolean).getOrElse(false)) {
    Some(ConfigManager.getAvroSchemaManagerConfig)
  } else {
    None
  }

  val colsIdxedFields: Seq[(Int, Field)] = schema
    .fieldNames
    .partition(x => rkFields.map(_.colName).contains(x))
    ._2.filter(catalog.sMap.exists).map(x => (schema.fieldIndex(x), catalog.getField(x)))

  val clusteringCfColumnsMap: Map[String, Seq[String]] = catalog.clusteringMap

  val enconder: ExpressionEncoder[Row] = RowEncoder.apply(schema).resolveAndBind()

  def getTableName(): TableName = TableName.valueOf(catalog.namespace + ":" + catalog.name)

  def convertToPut(row: Row): Put = {
    // construct bytes for row key
    val rowBytes = rkIdxedFields.map { case (x, y) =>
      Utils.toBytes(row(x), y, useSchemaAvroManager, darwinConf)
    }
    val rLen = rowBytes.foldLeft(0) { case (x, y) =>
      x + y.length
    }
    val rBytes = new Array[Byte](rLen)
    var offset = 0
    rowBytes.foreach { x =>
      System.arraycopy(x, 0, rBytes, offset, x.length)
      offset += x.length
    }
    // Removed timestamp.fold(new Put(rBytes))(new Put(rBytes, _))
    var put: Put = null
    try {
      put = new Put(rBytes)
    } catch {
      case e:IllegalArgumentException =>
        throw new IllegalArgumentException(s"Row length is 0: sparkRow: ${row.mkString}, SparkRowSchema: ${row.schema}, rkIdxedFields: $rkIdxedFields", e)
    }

    def isClusteringColumnFamily: Field => Boolean = (f: Field) => clusteringCfColumnsMap.contains(f.cf)
    def isStandardColumnFamily: Field => Boolean = (f: Field) => !isClusteringColumnFamily(f)

    // We generate put in 2 phase.
    // One for standard cf and static cells
    // One for cf with "clustering" feature (i.e. denormalization)
    colsIdxedFields.filter{
      case (_, f) => isStandardColumnFamily(f)
    }.foreach { case (colIndex, field) =>
      if (!row.isNullAt(colIndex)) {
        val b = Utils.toBytes(row(colIndex), field, useSchemaAvroManager, darwinConf)
        put.addColumn(Bytes.toBytes(field.cf), Bytes.toBytes(field.col), b)
      }
    }

    // Build a map to retrieve for the specific row the column qualifier prefix for cf with "clustering"
    val clusteringQualifierPrefixMap = clusteringCfColumnsMap.map{
      case (cf: String, clustFields: Seq[String]) =>
        val rowSchema = row.schema
        val cqPrefix = clustFields.map{
          fieldName =>
            row.getAs[String](rowSchema.fieldIndex(fieldName))
        }.mkString("|")
        cf -> cqPrefix
    }

    // Process denormalized columns
    colsIdxedFields.filter{
      // Take only field of clustering column family that are not in clustering fields
      case (_, f) => isClusteringColumnFamily(f) && !clusteringCfColumnsMap(f.cf).contains(f.col)
    }.foreach{
      case (colIndex, field) =>
        if (!row.isNullAt(colIndex)) {
          val cf = Bytes.toBytes(field.cf)
          // Compose final denormalized cq
          val finalCq = Bytes.toBytes(s"${clusteringQualifierPrefixMap(field.cf)}|${field.col}")
          val data = Utils.toBytes(row(colIndex), field, useSchemaAvroManager, darwinConf)

          put.addColumn(cf, finalCq, data)
        }
    }
    put
  }

  def convertToPut(internalRow: InternalRow): Put = {
    val row: Row = enconder.fromRow(internalRow)
    convertToPut(row)
  }
}