package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.nio.ByteOrder
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.darwin.connector.mock.MockConnector
import it.agilelab.darwin.manager.CachedEagerAvroSchemaManager
import it.agilelab.darwin.manager.util.ConfigurationKeys
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{Matchers, WordSpec}

class AvroEncodersSpec extends WordSpec with Matchers with CodegenTester {
  "RowToAvroExpression" must {

    val schema1: Schema                               = CompatibleClass.schema
    val toRecord1: CompatibleClass => GenericRecord   = CompatibleClass.toRecord
    val fromRecord1: GenericRecord => CompatibleClass = CompatibleClass.fromRecord

    val manager = new CachedEagerAvroSchemaManager(
      new MockConnector(
        ConfigFactory.empty().withValue(ConfigurationKeys.ENDIANNESS, ConfigValueFactory.fromAnyRef("BIG_ENDIAN"))
      ),
      ByteOrder.BIG_ENDIAN
    )

    def generateData(seed: Long, size: Int): List[CompatibleClass] = {
      val random = new scala.util.Random(seed)
      List.fill(size) {
        CompatibleClass(
          random.nextString(random.nextInt(15)),
          Some(random.nextDouble() * random.nextLong()),
          random.nextLong() -> random.nextString(random.nextInt(15))
        )
      }
    }

    "correctly handle serialization when not using darwin" in testAllCodegen {
      val elements = generateData(4, 1000)
      manager.registerAll(schema1 :: Nil)
      val encoder = AvroEncoders.avroEncoder(
        schema1,
        () => manager,
        toRecord1,
        fromRecord1
      )
      val df = spark.createDataset(elements)(encoder)

      assert(df.schema.fieldNames.toList == Seq("value"))
      val result = df.collect()
      assert(result sameElements elements)
    }
  }
}

case class CompatibleClass(a: String, b: Option[Double], c: (Long, String))
object CompatibleClass {
  val schema = SchemaBuilder
    .record("FromKafka")
    .fields()
    .name("a")
    .`type`()
    .stringType()
    .noDefault()
    .name("b")
    .`type`()
    .optional()
    .doubleType()
    .name("c")
    .`type`()
    .record("tuple_long_string")
    .fields()
    .requiredLong("_1")
    .requiredString("_2")
    .endRecord()
    .noDefault()
    .endRecord();

  def toRecord(data: CompatibleClass): GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("a", data.a)
    data.b.foreach(record.put("b", _))

    val tupleRecord = new GenericData.Record(schema.getField("c").schema())
    tupleRecord.put("_1", data.c._1)
    tupleRecord.put("_2", data.c._2)

    record.put("c", tupleRecord)
    record
  }

  def fromRecord(record: GenericRecord): CompatibleClass = {
    val a = record.get("a").toString
    val b = Option(record.get("b")).map(_.asInstanceOf[java.lang.Double].toDouble)
    val tupleRecord = record.get("c").asInstanceOf[GenericRecord]
    val c = (tupleRecord.get("_1").asInstanceOf[Long], tupleRecord.get("_2").toString)

    CompatibleClass(a, b, c)
  }
}
