package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.datasources.hbase.{Field, HBaseTableCatalog, Utils}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by Agile Lab s.r.l. on 04/11/2017.
  */
class HBaseSink(sparkSession: SparkSession, parameters: Map[String, String], hBaseContext: HBaseContext) extends Sink with Logging {

  val dynamicFields: Set[String] = parameters
    .get(HBaseSparkConf.DYNAMIC_FIELDS)
    .map(x => x.split("-", -1).toSet)
    .getOrElse(Set.empty[String])
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

  val dynamicFields: Set[String] = parameters
    .get(HBaseSparkConf.DYNAMIC_FIELDS)
    .map(x => x.split(":", -1).toSet)
    .getOrElse(Set.empty[String])

  @transient val rkFields: Seq[Field] = catalog.getRowKey
  val rkIdxedFields: Seq[(Int, Field)] = rkFields.map { case x =>
    (schema.fieldIndex(x.colName), x)
  }
  val colsIdxedFields: Seq[(Int, Field)] = schema
    .fieldNames
    .partition(x => rkFields.map(_.colName).contains(x))
    ._2.filter(catalog.sMap.exists).map(x => (schema.fieldIndex(x), catalog.getField(x)))
  val enconder: ExpressionEncoder[Row] = RowEncoder.apply(schema).resolveAndBind()

  def getTableName(): TableName = TableName.valueOf(catalog.namespace + ":" + catalog.name)

  def convertToPut(row: Row): Put = {
    // construct bytes for row key
    val rowBytes = rkIdxedFields.map { case (x, y) =>
      Utils.toBytes(row(x), y)
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
    val put = new Put(rBytes)

    colsIdxedFields.foreach { case (x, y) =>
      if(dynamicFields.contains(y.colName)){
        // Dynamic field case. Denormalize content in multiple columns
        val data = row(x)
        // Verify expected type matching
        val dataArray: Seq[Row] = data match {
          case x: Seq[Row] => x
          case _ => throw new Exception(s"Unsupported data type for dynamic Fields. Array of StructType Expected. " +
            s"Field ${y.colName} - ${y.dt}")
        }

        dataArray.foreach{
          dynRow =>
            val rowSchema = dynRow.schema
            val indexedFields = rowSchema
              .fieldNames
              .map(x => (x, rowSchema.fieldIndex(x))).toMap

            // Extract dynamic cq and payload to be written
            if(!indexedFields.contains(DynamicFieldStructure.COLUMN_QUALIFIER) || !indexedFields.contains(DynamicFieldStructure.PAYLOAD)){
              throw new Exception(s"Structures in dynamic fields array must contains keys cq and payload ")
            }
            val cqIndex: Int = indexedFields(DynamicFieldStructure.COLUMN_QUALIFIER)
            val dataIndex: Int = indexedFields(DynamicFieldStructure.PAYLOAD)
            val cqName = dynRow.getAs[String](cqIndex)
            val cq = Utils.toBytesPrimitiveType(cqName, rowSchema.fields(cqIndex).dataType)
            val dataType = if(y.avroSchema.isDefined) None else Some(dynRow.schema.fields(dataIndex).dataType.typeName)
            //TODO verify this Field init
            val dynamicFieldStruct = Field(cqName, y.cf,
              cqName,
              dataType,
              y.avroSchema, None, -1)
            val data = Utils.toBytes(dynRow.get(dataIndex), dynamicFieldStruct)

            // Update hbase put
            put.addColumn(Bytes.toBytes(y.cf), cq, data)
        }
      } else {
        if (!row.isNullAt(x)) {
          val b = Utils.toBytes(row(x), y)
          put.addColumn(Bytes.toBytes(y.cf), Bytes.toBytes(y.col), b)
        }
      }
    }
    put
  }

  def convertToPut(internalRow: InternalRow): Put = {
    val row: Row = enconder.fromRow(internalRow)
    convertToPut(row)
  }
}