package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util

import com.sksamuel.avro4s._
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{Column, Row}
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.ClassTag

class AvroDeserializerExpressionSpec extends WordSpec with Matchers with CodegenTester {

  def serializeElements(elements: Seq[UglyCaseClass]): Seq[Array[Byte]] = {
    elements.map { e =>
      val out     = new ByteArrayOutputStream()
      val avroOut = AvroOutputStream.binary[UglyCaseClass](out)
      avroOut.write(e)
      avroOut.flush()
      out.toByteArray
    }
  }

  def serializeUnboxed[T: ClassTag](elements: Seq[T], schema: Schema): Seq[Array[Byte]] = {
    import scala.collection.JavaConverters._
    elements.map { e =>
      val out     = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(out, null);
      val writer  = new GenericDatumWriter[AnyRef](schema);

      e match {
        case anArrayOfBytes: Array[Byte] =>
          writer.write(ByteBuffer.wrap(anArrayOfBytes), encoder)
        case anArrayOfInt: Array[Int] =>
          writer.write(anArrayOfInt.toList.asJava, encoder)
        case anArrayOfInt: Array[String] =>
          writer.write(anArrayOfInt.toList.asJava, encoder)
        case somethingElse: AnyRef =>
          writer.write(somethingElse, encoder);
        case _ =>
          throw new RuntimeException("should never get here")
      }
      encoder.flush()
      out.toByteArray
    }
  }

  def compareRowWithUglyClass(truth: UglyCaseClass, r: Row): Unit = {
    assert(truth.a sameElements r.getAs[Array[Byte]](0))
    assert(truth.b sameElements r.getSeq[Int](1))
    val naRow = r.getSeq[Row](2)
    assert(truth.na.length == naRow.length)
    truth.na.zip(naRow).foreach { case (a, b) => compareRowWithNestedClass(a, b) }
    assert(truth.d == new Date(r.getLong(3)))
    assert(truth.ts == new Timestamp(r.getLong(4)))
    compareRowWithNestedClass(truth.n, r.getStruct(5))
    assert(truth.sm == r.getMap[String, Int](6))
    assert(truth.som == r.getMap[String, java.lang.Double](7).mapValues(Option.apply))
    assert(truth.mm == r.getMap[String, Map[String, java.lang.Double]](8).mapValues(_.mapValues(Option.apply)))
    assert(truth.m == r.getMap[String, Row](9).mapValues(rowToNestedClass))
  }

  def compareRowWithNestedClass(a: NestedCaseClass, r: Row): Unit = {
    assert(a == rowToNestedClass(r))
  }

  def rowToNestedClass(r: Row): NestedCaseClass = {
    NestedCaseClass(
      r.getDouble(0),
      r.getLong(1),
      r.getString(2)
    )
  }

  "AvroToRowExpression" must {

    "correctly handle serialization when not using darwin" in testAllCodegen {

      import ss.implicits._

      val elements   = RowToAvroExpressionTestDataGenerator.generate(1L, 1000)
      val serialized = serializeElements(elements.toList)

      val df      = sc.parallelize(serialized, 4).toDF("serialized")
      val expr    = AvroDeserializerExpression($"serialized".expr, TestSchemas.schema.toString, None)
      val results = df.select(new Column(expr)).collect().map(_.getStruct(0))
      elements.zip(results).foreach { case (truth, res) => compareRowWithUglyClass(truth, res) }
    }

    "Handle values not boxed in generic record, Integer" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(new Integer(_))
      val schema     = SchemaBuilder.builder().intBuilder().endInt()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
      // resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "Handle values not boxed in generic record, Float" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(f => new java.lang.Float(f + 0.1))
      val schema     = SchemaBuilder.builder().floatBuilder().endFloat()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "Handle values not boxed in generic record, Double" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(f => new java.lang.Double(f + 0.1d))
      val schema     = SchemaBuilder.builder().doubleBuilder().endDouble()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "Handle values not boxed in generic record, Boolean" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(f => (f % 1) == 0)
      val schema     = SchemaBuilder.builder().booleanBuilder().endBoolean()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "Handle values not boxed in generic record, Bytes" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(f => Seq.range(0, f).map(_ % 100).map(_.toByte).toArray)
      val schema     = SchemaBuilder.builder().bytesBuilder().endBytes()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0)).map(_.asInstanceOf[Array[Byte]])

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(util.Arrays.equals(truth, result))
      }
    }

    "Handle values not boxed in generic record, Array[Int]" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(f => Seq.range(0, f).map(_ % 100).toArray)
      val schema     = SchemaBuilder.builder().array().items(SchemaBuilder.builder().intBuilder().endInt())
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0)).map(_.asInstanceOf[Seq[Int]])

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth.toSeq == result)
      }
    }

    "Handle values not boxed in generic record, Array[String]" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(f => Seq.range(0, f).map(_.toString).toArray)
      val schema     = SchemaBuilder.builder().array().items(SchemaBuilder.builder().stringBuilder().endString())
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0)).map(_.asInstanceOf[Seq[String]])

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth.toSeq == result)
      }
    }

    "Handle values not boxed in generic record, Long" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(Int.MaxValue.toLong, Int.MaxValue + 1000L).map(new java.lang.Long(_))
      val schema     = SchemaBuilder.builder().longBuilder().endLong()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "Handle values not boxed in generic record, String" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0, 1000).map(_.toString)
      val schema     = SchemaBuilder.builder().stringBuilder().endString()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "Handle values not boxed in generic record, Byte" in testAllCodegen {
      import ss.implicits._

      val elements   = Seq.range(0.toByte, Byte.MaxValue).map(new java.lang.Byte(_))
      val schema     = SchemaBuilder.builder().intBuilder().endInt()
      val serialized = serializeUnboxed(elements.toList, schema)

      val df       = sc.parallelize(serialized, 4).toDF("serialized")
      val expr     = AvroDeserializerExpression($"serialized".expr, schema.toString(), None)
      val resultDf = df.select(new Column(expr))
//       resultDf.queryExecution.debug.codegen()
      val data = resultDf.collect().map(_.get(0))

      elements.zip(data).foreach {
        case (truth, result) =>
          assert(truth === result)
      }
    }

    "correctly handle serialization when using darwin" in testAllCodegen {

      import ss.implicits._
      val elements   = RowToAvroExpressionTestDataGenerator.generate(1L, 1000)
      val serialized = serializeElements(elements)

      val df = sc.parallelize(serialized, 4).toDF("serialized")

      val darwinConf = ConfigFactory.parseString("""
          |type: cached_eager
          |connector: "mock"
          |endianness: "BIG_ENDIAN"
        """.stripMargin)

      val expr    = AvroDeserializerExpression($"serialized".expr, TestSchemas.schema.toString, Some(darwinConf))
      val results = df.select(new Column(expr)).collect().map(_.getStruct(0))
      elements.zip(results).foreach { case (truth, res) => compareRowWithUglyClass(truth, res) }
    }

    "correctly handle null" in testAllCodegen {
      val darwinConf = ConfigFactory.parseString("""
          |type: cached_eager
          |connector: "mock"
          |endianness: "LITTLE_ENDIAN"
        """.stripMargin)
      val child      = Literal(null, BinaryType)
      val expr1      = AvroDeserializerExpression(child, TestSchemas.schema.toString, Some(darwinConf))
      val expr2      = AvroDeserializerExpression(child, TestSchemas.schema.toString, None)
      val res        = ss.range(1).select(new Column(expr1), new Column(expr2)).collect()
      assert(res sameElements Array(Row(null, null)))
    }
  }
}
