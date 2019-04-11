package it.agilelab.bigdata.wasp.consumers.spark.utils

import com.sksamuel.avro4s._
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.utils.SparkTestKit
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import it.agilelab.darwin.manager.util.AvroSingleObjectEncodingUtils
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, WordSpec}

class AvroConverterExpressionSpec extends WordSpec
  with Matchers
  with SparkTestKit {

  "RowToAvroExpression" must {

    "correctly handle serialization when not using darwin" in {

      import ss.implicits._

      val elements = RowToAvroExpressionTestDataGenerator.generate(1L, 1000)

      val df = sc.parallelize(elements, 4).toDF()

      val child = struct(df.columns.map(df.col): _*).expr

      val expr = AvroConverterExpression(Some(TestSchemas.schema.toString), "pippo", "wasp")(child, df.schema)

      val results = df.select(new Column(expr)).collect().map(r => r.get(0)).flatMap { data =>
        import TestSchemas.implicits._

        AvroInputStream.binary[UglyCaseClass](data.asInstanceOf[Array[Byte]]).iterator.toSeq
      }

      elements.zip(results).foreach {
        case(UglyCaseClass(a1,z1, y1, b1, c1, d1), UglyCaseClass(a2,z2, y2, b2, c2, d2)) =>
          assert(a1 sameElements a2)
          assert(b1 == b2)
          assert(c1==c2)
          assert(d1==d2)
          assert(z1 sameElements z2)
          assert(y1 sameElements y2)
      }
    }

    "correctly handle serialization when using darwin" in {

      import ss.implicits._

      val elements = RowToAvroExpressionTestDataGenerator.generate(1L, 1000)

      val df = sc.parallelize(elements, 4).toDF()

      val child = struct(df.columns.map(df.col): _*).expr

      val darwinConf = ConfigFactory.parseString(
        """
          |type: cached_eager
          |connector: "mock"
        """.stripMargin)

      AvroSchemaManagerFactory.initialize(darwinConf)

      val expr = AvroConverterExpression(darwinConf, TestSchemas.schema, "pippo", "wasp")(child, df.schema)

      val results = df.select(new Column(expr)).collect().map(r => r.get(0)).flatMap { data =>
        import TestSchemas.implicits._
        val element = AvroSingleObjectEncodingUtils.dropHeader(data.asInstanceOf[Array[Byte]])
        AvroInputStream.binary[UglyCaseClass](element).iterator.toSeq
      }

      elements.zip(results).foreach {
        case (UglyCaseClass(a1, z1, y1, b1, c1, d1), UglyCaseClass(a2, z2, y2, b2, c2, d2)) =>
          assert(a1 sameElements a2)
          assert(b1 == b2)
          assert(c1 == c2)
          assert(d1 == d2)
          assert(z1 sameElements z2)
          assert(y1 sameElements y2)
      }
    }

    "correctly handle null" in {
      val darwinConf = ConfigFactory.parseString(
        """
          |type: cached_eager
          |connector: "mock"
        """.stripMargin)
      AvroSchemaManagerFactory.initialize(darwinConf)
      val schema = StructType(Seq(StructField("_1", IntegerType, true), StructField("_2", StringType)))
      val avroSchema = AvroSchema[(Int, String)]
      val child = Literal(null, schema)
      val expr1 = AvroConverterExpression(Some(avroSchema.toString), "pippo", "wasp")(child, schema)
      val expr2 = AvroConverterExpression(darwinConf, avroSchema, "pippo", "wasp")(child, schema)
      val res = ss.range(1).select(new Column(expr1), new Column(expr2)).collect()
      assert(res sameElements Array(Row(null, null)))
    }
  }
}
