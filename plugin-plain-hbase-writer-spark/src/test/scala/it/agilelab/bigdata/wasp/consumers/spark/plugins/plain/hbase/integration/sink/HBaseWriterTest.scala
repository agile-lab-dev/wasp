package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink

import it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.TestFixture
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll


class HBaseWriterTest extends TestFixture with BeforeAndAfterAll{

  val spark = sparkSession

  override def afterAll(): Unit = {
    spark.stop()
  }

  "HBaseWriter" should {

    "validate input dataframe schema" in {
      val simpleData = Seq(
        Row(HBaseWriterProperties.UpsertOperation, "aaa".getBytes(), "col".getBytes(), Map("value".getBytes() -> "v".getBytes()))
      )

      val expectedSchema = StructType(
        Array(
          StructField(HBaseWriterProperties.OperationAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.RowkeyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ColumnFamilyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ValuesAttribute, MapType(BinaryType, BinaryType), nullable = false)
        )
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), expectedSchema)

      HBaseWriter
        .validateQuery(df.schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
    }

    "launch exception if rowKey is not a Byte Array" in {
      val simpleData = Seq(
        Row(HBaseWriterProperties.UpsertOperation, "aaa", "col".getBytes(), Map("value".getBytes() -> "v".getBytes()))
      )

      val expectedSchema = StructType(
        Array(
          StructField(HBaseWriterProperties.OperationAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.RowkeyAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.ColumnFamilyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ValuesAttribute, MapType(BinaryType, BinaryType), nullable = false)
        )
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), expectedSchema)

      val caught = intercept[IllegalStateException] {
        HBaseWriter
          .validateQuery(df.schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
      }

      caught.getMessage shouldEqual "rowKey attribute unsupported type string. It must be a BinaryType"
    }

    "launch exception if columnFamily is not a Byte Array" in {
      val simpleData = Seq(
        Row(HBaseWriterProperties.UpsertOperation, "aaa".getBytes(), "col", Map("value".getBytes() -> "v".getBytes()))
      )

      val expectedSchema = StructType(
        Array(
          StructField(HBaseWriterProperties.OperationAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.RowkeyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ColumnFamilyAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.ValuesAttribute, MapType(BinaryType, BinaryType), nullable = false)
        )
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), expectedSchema)

      val caught = intercept[IllegalStateException] {
        HBaseWriter
          .validateQuery(df.schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
      }

      caught.getMessage shouldEqual "columnFamily attribute unsupported type string. It must be a BinaryType"
    }


    "launch exception if values is not in the right format" in {
      val simpleData = Seq(
        Row(HBaseWriterProperties.UpsertOperation, "aaa".getBytes(), "col", Map("value" -> "v"))
      )

      val expectedSchema = StructType(
        Array(
          StructField(HBaseWriterProperties.OperationAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.RowkeyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ColumnFamilyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ValuesAttribute, MapType(StringType, StringType), nullable = false)
        )
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), expectedSchema)

      val caught = intercept[IllegalStateException] {
        HBaseWriter
          .validateQuery(df.schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
      }

      caught
        .getMessage shouldEqual "values attribute unsupported type map<string,string>. It must be a MapType(BinaryType,BinaryType,true)"
    }

    "launch exception if required field is not present" in {
      val simpleData = Seq(
        Row("aaa".getBytes(), "col", Map("value" -> "v"))
      )

      val expectedSchema = StructType(
        Array(
          StructField(HBaseWriterProperties.RowkeyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ColumnFamilyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ValuesAttribute, MapType(StringType, StringType), nullable = false)
        )
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), expectedSchema)

      val caught = intercept[IllegalStateException] {
        HBaseWriter
          .validateQuery(df.schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
      }

      caught
        .getMessage shouldEqual "operation is mandatory"
    }

    "validate input dataframe if missing a non mandatory field" in {
      val simpleData = Seq(
        Row(HBaseWriterProperties.UpsertOperation, "aaa".getBytes(), "col")
      )

      val expectedSchema = StructType(
        Array(
          StructField(HBaseWriterProperties.OperationAttribute, StringType, nullable = false),
          StructField(HBaseWriterProperties.RowkeyAttribute, BinaryType, nullable = false),
          StructField(HBaseWriterProperties.ColumnFamilyAttribute, BinaryType, nullable = false)
        )
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), expectedSchema)

      HBaseWriter
          .validateQuery(df.schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
    }


  }

}
