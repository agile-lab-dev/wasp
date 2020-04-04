package org.apache.hadoop.hbase.spark

import it.agilelab.bigdata.wasp.consumers.spark.utils.AvroSerializerExpression
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.avro.Schema
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.datasources.hbase.{Field, HBaseTableCatalog, Utils}
import org.apache.spark.sql.types.StructType

case class PutConverterFactory(@transient catalog: HBaseTableCatalog,
                               schema: StructType) {
  @transient val rkFields: Seq[Field] = catalog.getRowKey
  val rkIdxedFields: Seq[(Int, Field)] = rkFields.map { x =>
    (schema.fieldIndex(x.colName), x)
  }

  assert(rkIdxedFields.nonEmpty,s"""
                                   |You should put at least one column for the key of hbase row in the tableCatalog of the KeyValueModel. ex:
                                   |"id":{"cf":"rowkey", "col":"key", "type":"string"}
                                   |""".stripMargin
  )

  val colsIdxedFields: Seq[(Int, Field)] = schema
    .fieldNames
    .partition(x => rkFields.map(_.colName).contains(x))
    ._2.filter(catalog.sMap.exists).map(x => (schema.fieldIndex(x), catalog.getField(x)))

  val clusteringCfColumnsMap: Map[String, Seq[String]] = catalog.clusteringMap

  val enconder: ExpressionEncoder[Row] = RowEncoder.apply(schema).resolveAndBind()

  def getTableName(): TableName = TableName.valueOf(catalog.namespace + ":" + catalog.name)

  def convertToPut(row: InternalRow): Put = {
    // construct bytes for row key
    val rowBytes = rkIdxedFields.map { case (x, y) =>
      Utils.toBytesPrimitiveType(row.get(x, schema(x).dataType), y.dt)
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
        throw new IllegalArgumentException(s"Row length is 0, rkIdxedFields: $rkIdxedFields", e)
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
        val b = Utils.toBytesPrimitiveType(row.get(colIndex, schema(colIndex).dataType), field.dt)
        put.addColumn(Bytes.toBytes(field.cf), Bytes.toBytes(field.col), b)
      }
    }

    // Build a map to retrieve for the specific row the column qualifier prefix for cf with "clustering"
    val clusteringQualifierPrefixMap = clusteringCfColumnsMap.map{
      case (cf: String, clustFields: Seq[String]) =>
        val cqPrefix = clustFields.map { fieldName =>
          row.getString(schema.fieldIndex(fieldName))
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
          val data = Utils.toBytesPrimitiveType(row.get(colIndex, schema(colIndex).dataType), field.dt)

          put.addColumn(cf, finalCq, data)
        }
    }
    put
  }
}

object PutConverterFactory {
  def apply(parameters: Map[String, String], data: DataFrame): PutConverterFactory = {
    PutConverterFactory(getCatalog(parameters), data.schema)
  }

  private def getCatalog(parameters: Map[String, String]): HBaseTableCatalog = HBaseTableCatalog(parameters)

  private def getFields(catalog: HBaseTableCatalog, schema: StructType): (Seq[Field], Seq[Field]) = {
    val rkFields: Seq[Field] = catalog.getRowKey
    val rkColumnsSet = rkFields.map(_.colName).toSet
    val colsFields: Seq[Field] = schema
      .fieldNames
      .filterNot(rkColumnsSet)
      .filter(catalog.sMap.exists)
      .map(catalog.getField)
    (rkFields, colsFields)
  }

  def convertAvroColumns(parameters: Map[String, String], data: DataFrame): DataFrame = {
    val catalog = getCatalog(parameters)
    val (keyFields, colFields) = getFields(catalog, data.schema)
    val (toAvroFields, otherFields) = (keyFields ++ colFields).partition(_.avroSchema.isDefined)
    val toAvroColumns = toAvroFields.map { f =>
      val avroConverterExpressionGenerator: (Expression, StructType) => AvroSerializerExpression =
        if (catalog.get("useAvroSchemaManager").exists(_.toBoolean)) {
          val avroSchema = new Schema.Parser().parse(f.avroSchema.get)
          AvroSerializerExpression(
            ConfigManager.getAvroSchemaManagerConfig, avroSchema, f.colName, "wasp")
        } else {
          AvroSerializerExpression(f.avroSchema, f.colName, "wasp")
        }
      // Generating the column with the encoded avro with the same name of the original column
      new Column(avroConverterExpressionGenerator(
        data.col(f.colName).expr,
        data.schema(f.colName).dataType.asInstanceOf[StructType])).as(f.colName)
    }
    val otherColumns = otherFields.map(f => data.col(f.colName))
    data.select(toAvroColumns ++ otherColumns: _*)
  }
}
