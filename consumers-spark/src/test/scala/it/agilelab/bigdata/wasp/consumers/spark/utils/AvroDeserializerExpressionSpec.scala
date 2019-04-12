package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayOutputStream
import java.sql.{Date, Timestamp}

import com.sksamuel.avro4s._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{Column, Row}
import org.scalatest.{Matchers, WordSpec}

class AvroDeserializerExpressionSpec extends WordSpec
  with Matchers
  with CodegenTester {

  def serializeElements(elements: Seq[UglyCaseClass]): Seq[Array[Byte]] = {
    elements.map { e =>
      import TestSchemas.implicits._
      val out = new ByteArrayOutputStream()
      val avroOut = AvroOutputStream.binary[UglyCaseClass](out)
      avroOut.write(e)
      avroOut.flush()
      out.toByteArray
    }
  }

  def compareRowWithUglyClass(truth: UglyCaseClass, r: Row) = {
    assert(truth.a sameElements r.getAs[Array[Byte]](0))
    assert(truth.b sameElements r.getSeq[Int](1))
    val naRow = r.getSeq[Row](2)
    assert(truth.na.length == naRow.length)
    truth.na.zip(naRow).foreach { case (a, b) => compareRowWithNestedClass(a, b) }
    assert(truth.d == new Date(r.getLong(3)))
    assert(truth.ts == new Timestamp(r.getLong(4)))
    compareRowWithNestedClass(truth.n, r.getStruct(5))
  }

  def compareRowWithNestedClass(a: NestedCaseClass, r: Row) = {
    assert(a.d == r.getDouble(0))
    assert(a.l == r.getLong(1))
    assert(a.s == r.getString(2))
  }

  "AvroToRowExpression" must {

    "correctly handle serialization when not using darwin" in testAllCodegen {

      import ss.implicits._

      val elements = RowToAvroExpressionTestDataGenerator.generate(1L, 1000)
      val serialized = serializeElements(elements.toList)

      val df = sc.parallelize(serialized, 4).toDF("serialized")
      val expr = AvroDeserializerExpression($"serialized".expr, TestSchemas.schema.toString, None)
      val results = df.select(new Column(expr)).collect().map(_.getStruct(0))
      elements.zip(results).foreach { case (truth, res) => compareRowWithUglyClass(truth, res) }
    }

    "correctly handle serialization when using darwin" in testAllCodegen {

      import ss.implicits._
      val elements = RowToAvroExpressionTestDataGenerator.generate(1L, 1000)
      val serialized = serializeElements(elements)

      val df = sc.parallelize(serialized, 4).toDF("serialized")

      val darwinConf = ConfigFactory.parseString(
        """
          |type: cached_eager
          |connector: "mock"
        """.stripMargin)

      val expr = AvroDeserializerExpression($"serialized".expr, TestSchemas.schema.toString, Some(darwinConf))
      val results = df.select(new Column(expr)).collect().map(_.getStruct(0))
      elements.zip(results).foreach { case (truth, res) => compareRowWithUglyClass(truth, res) }
    }

    "correctly handle null" in testAllCodegen {
      val darwinConf = ConfigFactory.parseString(
        """
          |type: cached_eager
          |connector: "mock"
        """.stripMargin)
      val child = Literal(null, BinaryType)
      val expr1 = AvroDeserializerExpression(child, TestSchemas.schema.toString, Some(darwinConf))
      val expr2 = AvroDeserializerExpression(child, TestSchemas.schema.toString, None)
      val res = ss.range(1).select(new Column(expr1), new Column(expr2)).collect()
      assert(res sameElements Array(Row(null, null)))
    }
  }
}
