package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.sql.{Date, Timestamp}

import com.sksamuel.avro4s._
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.utils.SparkTestKit
import org.apache.avro.Schema
import org.apache.spark.sql.Column
import org.scalatest.{Matchers, WordSpec}
import org.apache.avro.Schema.Field

import scala.util.Random




object RowToAvroExpressionTestDataGenerator {


  def generate(seed: Long, elements: Int): Seq[UglyCaseClass] = {

    //random number generator with fixed seed to have reproducible tests
    val random = new Random(seed)

    Iterator.continually {
      generate(random)
    }.take(elements).toSeq

  }

  def generate(random: Random): UglyCaseClass = {

    UglyCaseClass(generateByteArray(random),
      generateIntArray(random),
      Iterator.continually(generateNestedCaseClass(random)).take(10).toArray,
      generateDate(random),
      generateTimestamp(random),
      generateNestedCaseClass(random))

  }

  def generateByteArray(random: Random): Array[Byte] = {
    val array = new Array[Byte](25)
    random.nextBytes(array)
    array
  }

  def generateIntArray(random: Random): Array[Int] = {
    Iterator.continually(random.nextInt()).take(10).toArray
  }

  val freezeNow = System.currentTimeMillis()

  def generateTimestamp(random: Random): Timestamp =
    new Timestamp(freezeNow)

  def generateDate(random: Random): Date =
    new Date(freezeNow)


  def generateNestedCaseClass(random: Random): NestedCaseClass =
    NestedCaseClass(random.nextDouble(), random.nextLong(), random.nextString(30))


  def generateMap(random: Random): Map[String, Int] =
    Iterator.continually {
      (random.nextString(20), random.nextInt())
    }.take(20).toMap
}


class AvroConverterExpressionSpec extends WordSpec
  with Matchers
  with SparkTestKit {

  "RowToAvroExpression" must {

    "correctly handle" in {

      import ss.implicits._

      val elements = RowToAvroExpressionTestDataGenerator.generate(1L, 20)

      val df = sc.parallelize(elements).toDF()




      val expr = AvroConverterExpression(
        children = df.columns.map(df.col).map(_.as("serialized").expr).toSeq,
        schemaAvroJson = Some(SchemaHolder.jsonSchema),
        avroSchemaManagerConfig = Some(ConfigFactory.empty()),
        useAvroSchemaManager = false,
        inputSchema = df.schema,
        structName = "pippo",
        namespace = "wasp",
        fieldsToWrite = None,
        timeZoneId = None)



        val results = df.select(new Column(expr)).collect().map(r => r.get(0)).flatMap{ data =>


          implicit object DateTimeToValue extends ToValue[Date] {
            override def apply(value: Date): Long = value.getTime
          }

          implicit object DateTimeFromValue extends FromValue[Date] {
            override def apply(value: Any, field: Field): Date = new Date(value.asInstanceOf[Long])
          }

          implicit object TimestampToValue extends ToValue[Timestamp] {
            override def apply(value: Timestamp): Long = value.getTime
          }

          implicit object TimestampFromValue extends FromValue[Timestamp] {
            override def apply(value: Any, field: Field): Timestamp = new Timestamp(value.asInstanceOf[Long])
          }

          implicit val schemaFor: SchemaFor[UglyCaseClass] = new  SchemaFor[UglyCaseClass] {
            override def apply(): Schema = new Schema.Parser().parse(SchemaHolder.jsonSchema)
          }
          implicit val record: FromRecord[UglyCaseClass] = FromRecord[UglyCaseClass]

          AvroInputStream.binary[UglyCaseClass](data.asInstanceOf[Array[Byte]]).iterator.toSeq
        }




        elements.zip(results).foreach {
          case(UglyCaseClass(a1,z1, y1, b1, c1, d1), UglyCaseClass(a2,z2, y2, b2, c2, d2)) =>
            assert(a1 sameElements a2)
            assert(b1.toLocalDate==b2.toLocalDate)
            assert(c1==c2)
            assert(d1==d2)
            assert(z1 sameElements z2)
            assert(y1 sameElements y2)
        }

    }

  }


}
