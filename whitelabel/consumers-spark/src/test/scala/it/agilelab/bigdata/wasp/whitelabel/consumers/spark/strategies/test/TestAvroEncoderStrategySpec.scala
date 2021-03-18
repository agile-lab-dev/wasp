package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test

import java.io.{ByteArrayInputStream, DataInputStream, InputStream}
import java.nio.ByteOrder

import com.google.common.io.ByteStreams
import com.sksamuel.avro4s.{AvroSchema, FromRecord}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestNestedDocument, TestState}
import it.agilelab.darwin.common.Logging
import it.agilelab.darwin.connector.mock.MockConnector
import it.agilelab.darwin.manager.CachedEagerAvroSchemaManager
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class TestAvroEncoderStrategySpec extends FlatSpec with Matchers with BeforeAndAfterEach with Logging {

  private val master = "local"

  private val appName = "Test"

  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = new SparkSession.Builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  /**
    * The test checks if the state generated in it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestAvroEncoderStrategy
    * (in flatMapGroupsWithState) is correct. The file 3.delta has been copied from hdfs to resource directory in order to deserialize it and
    * checks its contents. The state is in Avro format and compressed using LZ4 algorithm.
    */
  it should "generate a valid delta state" in {

    val dataInputStream: DataInputStream = TestAvroEncoderStrategySpec.decompressLz4(
      spark.sparkContext.hadoopConfiguration,
      spark.sparkContext.getConf,
      new Path("whitelabel/consumers-spark/src/test/resources/3.delta")
    )

    val (keyVal: Long, avroFile: Array[Byte]) = TestAvroEncoderStrategySpec.extractData(dataInputStream)

    Bytes.toHex(avroFile).take(4) shouldBe "c301"

    val testState: TestState = TestAvroEncoderStrategySpec.fromByteArray(avroFile)

    testState shouldBe TestState(1, List(TestNestedDocument("field1_38", 38, Some("field3_38"))), "1")

    keyVal shouldBe testState.list.head.field2
  }

}

object TestAvroEncoderStrategySpec {

  def decompressLz4(hadoopConf: Configuration, sparkConf: SparkConf, filePath: Path): DataInputStream = {
    val fs                      = FileSystem.newInstance(hadoopConf)
    val is                      = fs.open(filePath)
    val compressed: InputStream = new LZ4CompressionCodec(sparkConf).compressedInputStream(is)
    new DataInputStream(compressed)
  }

  def extractData(dataInputStream: DataInputStream): (Long, Array[Byte]) = {
    val keySize: Int = dataInputStream.readInt()
    val keyRowBuffer = new Array[Byte](keySize)
    ByteStreams.readFully(dataInputStream, keyRowBuffer, 0, keySize)
    val keyRow = new UnsafeRow(1)
    keyRow.pointTo(keyRowBuffer, keySize)
    val keyValue = keyRow.getUTF8String(0).toString.toLong

    val valueSize      = dataInputStream.readInt()
    val valueRowBuffer = new Array[Byte](valueSize)
    ByteStreams.readFully(dataInputStream, valueRowBuffer, 0, valueSize)
    val valueRow = new UnsafeRow(1)
    valueRow.pointTo(valueRowBuffer, valueSize)
    val avroFile: Array[Byte] = valueRow.getBinary(0)

    (keyValue, avroFile)
  }

  def fromByteArray(byteArray: Array[Byte]): TestState = {
    val manager: CachedEagerAvroSchemaManager =
      new CachedEagerAvroSchemaManager(new MockConnector(ConfigFactory.empty()), ByteOrder.BIG_ENDIAN)

    val schema: Schema = AvroSchema[TestState]
    manager.registerAll(Seq(schema))
    val genericRecord = new GenericData.Record(schema)

    val inputStream = new ByteArrayInputStream(byteArray)
    val decoder     = DecoderFactory.get().binaryDecoder(inputStream, null)

    val writerSchema = manager.extractSchema(inputStream).right.get
    val reader       = new GenericDatumReader[GenericRecord](writerSchema, schema)
    FromRecord[TestState].apply(reader.read(genericRecord, decoder))
  }
}
