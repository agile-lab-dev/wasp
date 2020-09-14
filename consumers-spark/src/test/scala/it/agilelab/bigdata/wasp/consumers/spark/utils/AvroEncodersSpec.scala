package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.nio.ByteOrder

import com.sksamuel.avro4s.{AvroSchema, FromRecord, ToRecord}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.darwin.connector.mock.MockConnector
import it.agilelab.darwin.manager.CachedEagerAvroSchemaManager
import it.agilelab.darwin.manager.util.ConfigurationKeys
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.{Matchers, WordSpec}

class AvroEncodersSpec extends WordSpec with Matchers with CodegenTester {
  "RowToAvroExpression" must {

    val schema1: Schema                               = AvroSchema[CompatibleClass]
    val toRecord1: CompatibleClass => GenericRecord   = ToRecord[CompatibleClass].apply(_)
    val fromRecord1: GenericRecord => CompatibleClass = FromRecord[CompatibleClass].apply(_)

    val manager = new CachedEagerAvroSchemaManager(new MockConnector(ConfigFactory.empty().withValue(ConfigurationKeys.ENDIANNESS,ConfigValueFactory.fromAnyRef("BIG_ENDIAN"))), ByteOrder.BIG_ENDIAN)

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
      val df = ss.createDataset(elements)(encoder)

      assert(df.schema.fieldNames.toList == Seq("value"))
      val result = df.collect()
      assert(result sameElements elements)
    }
  }
}

case class CompatibleClass(a: String, b: Option[Double], c: (Long, String))
