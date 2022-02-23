package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration.sink.HBaseWriterProperties
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseTestingUtility, HConstants, TableName}
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/***
  * Test suit that runs integration test using an embedded hbase, it is very useful to test if the plugin works
  * but unluckily it is not stable in the ci environment, also, hbase-testing-utility has runtime library conflict using
  * vanilla flavours, so it can be run only with CDP or CDH flavours
  */
@Ignore
class DefaultSourceTest extends TestFixture with BeforeAndAfterAll{

  val hbaseTestUtils = new HBaseTestingUtility

  private val logger = LoggerFactory.getLogger(classOf[DefaultSourceTest])

  case class Test(operation: String,
                  rowKey: Array[Byte],
                  columnFamily: Array[Byte],
                  values: Map[Array[Byte], Array[Byte]])

  override def beforeAll(): Unit = {
    try {
      hbaseTestUtils.getConfiguration.addResource("hbase/hbase-site-local.xml")
      hbaseTestUtils.getConfiguration.addResource("hbase/hbase-policy-local.xml")
      hbaseTestUtils.getConfiguration.reloadConfiguration()
      hbaseTestUtils.startMiniCluster
      new HBaseContext(sqlContext.sparkContext, hbaseTestUtils.getConfiguration)
    } catch {
      case e: Exception => logger.error("Unable to start hbase mini cluster", e)
    }

  }

  val spark = sparkSession

  import spark.implicits._

  implicit val sqlContext = spark.sqlContext

  "DefaultSource" should {

    "write data to hbase" in {
      val connection = ConnectionFactory.createConnection(hbaseTestUtils.getConfiguration)

      val tableName = "table"
      val columnFamily = "test".getBytes()
      createTable(connection, tableName, columnFamily)

      val data = List(Test(HBaseWriterProperties.UpsertOperation,
        "1".getBytes(),
        columnFamily,
        Map("value".getBytes() -> "v1".getBytes(), "newValue".getBytes() -> "newValue1".getBytes())),
        Test(HBaseWriterProperties.UpsertOperation,
          "2".getBytes(),
          columnFamily,
          Map("value".getBytes() -> "v2".getBytes()))
      )


      val stream: MemoryStream[Test] = MemoryStream[Test]

      val streamingQuery = stream
        .toDF()
        .writeStream
        .option("tableName", tableName)
        .option("hbase.spark.use.hbasecontext", "true")
        .option("checkpointLocation", checkpointLocation)
        .format("it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration")
        .start

      val offset = stream.addData(data)

      streamingQuery.awaitTermination(2000)
      streamingQuery.stop()
      stream.commit(offset.asInstanceOf[LongOffset])

      val hTable = connection.getTable(TableName.valueOf(tableName))

      val result1 = hTable.get(new Get("1".getBytes()))
      val result2 = hTable.get(new Get("2".getBytes()))

      result1.isEmpty shouldEqual false
      result2.isEmpty shouldEqual false

      val qualifiers1 = result1.getFamilyMap(columnFamily)
        .asScala
        .map { case (k, v) => new String(k) -> new String(v) }
      val qualifiers2 = result2.getFamilyMap(columnFamily)
        .asScala
        .map { case (k, v) => new String(k) -> new String(v) }

      qualifiers1.contains("value") shouldEqual true
      qualifiers1.contains("newValue") shouldEqual true
      qualifiers2.contains("value") shouldEqual true

      qualifiers1("value") shouldEqual "v1"
      qualifiers1("newValue") shouldEqual "newValue1"
      qualifiers2("value") shouldEqual "v2"
    }

    "do not insert data without qualifier" in {
      val connection = ConnectionFactory.createConnection(hbaseTestUtils.getConfiguration)

      val tableName = "table"
      val columnFamily = "test".getBytes()
      val rowKey = "10".getBytes()

      createTable(connection, tableName, columnFamily)

      val data = List(Test(HBaseWriterProperties.UpsertOperation,
        rowKey,
        columnFamily,
        Map.empty)
      )


      val stream: MemoryStream[Test] = MemoryStream[Test]

      val streamingQuery = stream
        .toDF()
        .writeStream
        .option("tableName", tableName)
        .option("hbase.spark.use.hbasecontext", "true")
        .option("checkpointLocation", checkpointLocation)
        .format("it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration")
        .start

      val offset = stream.addData(data)

      streamingQuery.awaitTermination(2000)
      streamingQuery.stop()
      stream.commit(offset.asInstanceOf[LongOffset])

      val hTable = connection.getTable(TableName.valueOf(tableName))

      val result1 = hTable.get(new Get(rowKey))

      result1.isEmpty shouldEqual true
    }

    "delete cell" in {
      val connection = ConnectionFactory.createConnection(hbaseTestUtils.getConfiguration)

      val tableName = "table"
      val columnFamily = "test".getBytes()
      val rowKey = "20".getBytes()

      createTable(connection, tableName, columnFamily)

      val data = List(Test(HBaseWriterProperties.UpsertOperation,
        rowKey,
        columnFamily,
        Map("value".getBytes() -> "v1".getBytes(), "newValue".getBytes() -> "newValue1".getBytes())),
        Test(HBaseWriterProperties.DeleteCellOperation,
          rowKey,
          columnFamily,
          Map("newValue".getBytes() -> "newValue1".getBytes()))
      )

      val stream: MemoryStream[Test] = MemoryStream[Test]

      val streamingQuery = stream
        .toDF()
        .writeStream
        .option("tableName", tableName)
        .option("checkpointLocation", checkpointLocation)
        .option("hbase.spark.use.hbasecontext", "true")
        .format("it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration")
        .start

      val offset = stream.addData(data)

      streamingQuery.awaitTermination(2000)
      streamingQuery.stop()
      stream.commit(offset.asInstanceOf[LongOffset])

      val hTable = connection.getTable(TableName.valueOf(tableName))

      val result = hTable.get(new Get(rowKey))

      result.isEmpty shouldEqual false
      val qualifiers = result.getFamilyMap(columnFamily).asScala.map { case (k, v) => new String(k) -> new String(v) }

      qualifiers.contains("value") shouldEqual true
      qualifiers.contains("newValue") shouldEqual false

      qualifiers("value") shouldEqual "v1"
    }

    "delete row" in {
      val connection = ConnectionFactory.createConnection(hbaseTestUtils.getConfiguration)

      val tableName = "table"
      val columnFamily = "test".getBytes()
      val rowKey = "30".getBytes()

      createTable(connection, tableName, columnFamily)

      val data = List(Test(HBaseWriterProperties.UpsertOperation,
        rowKey,
        columnFamily,
        Map("value".getBytes() -> "v".getBytes())),
        Test(HBaseWriterProperties.DeleteRowOperation, rowKey, "".getBytes(), Map.empty)
      )

      val stream: MemoryStream[Test] = MemoryStream[Test]

      val streamingQuery = stream
        .toDF()
        .writeStream
        .option("tableName", tableName)
        .option("hbase.spark.use.hbasecontext", "true")
        .option("checkpointLocation", checkpointLocation)
        .format("it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration")
        .start

      val offset = stream.addData(data)

      streamingQuery.awaitTermination(2000)
      streamingQuery.stop()
      stream.commit(offset.asInstanceOf[LongOffset])

      val hTable = connection.getTable(TableName.valueOf(tableName))

      val result = hTable.get(new Get(rowKey))
      result.isEmpty shouldEqual true
    }

    "write to hbase and create hbase table if not exists" in {
      val tableName = "testTable"
      val columnFamily = "testFamily"
      val columnFamily2 = "testFamily2"

      val data = List(Test(HBaseWriterProperties.UpsertOperation,
        "1".getBytes(),
        columnFamily.getBytes(),
        Map("value".getBytes() -> "v1".getBytes())),
        Test(HBaseWriterProperties.UpsertOperation,
          "2".getBytes(),
          columnFamily2.getBytes(),
          Map("value".getBytes() -> "v2".getBytes()))
      )


      val stream: MemoryStream[Test] = MemoryStream[Test]

      val streamingQuery = stream
        .toDF()
        .writeStream
        .option("newtable", 4)
        .option("tableName", tableName)
        .option("columnFamilies", s"$columnFamily,$columnFamily2")
        .option("hbase.spark.use.hbasecontext", "true")
        .option("checkpointLocation", checkpointLocation)
        .format("it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration")
        .start

      val offset = stream.addData(data)

      streamingQuery.awaitTermination(2000)
      streamingQuery.stop()
      stream.commit(offset.asInstanceOf[LongOffset])

      val connection = ConnectionFactory.createConnection(hbaseTestUtils.getConfiguration)
      val hTable = connection.getTable(TableName.valueOf(tableName))

      val result1 = hTable.get(new Get("1".getBytes()))
      val result2 = hTable.get(new Get("2".getBytes()))

      result1.isEmpty shouldEqual false
      result2.isEmpty shouldEqual false

      val qualifiers1 = result1.getFamilyMap(columnFamily.getBytes())
        .asScala
        .map { case (k, v) => new String(k) -> new String(v) }
      val qualifiers2 = result2.getFamilyMap(columnFamily2.getBytes())
        .asScala
        .map { case (k, v) => new String(k) -> new String(v) }

      qualifiers1.contains("value") shouldEqual true
      qualifiers2.contains("value") shouldEqual true

      qualifiers1("value") shouldEqual "v1"
      qualifiers2("value") shouldEqual "v2"
    }

  }

  private def createTable(connection: Connection,
                          tableName: String,
                          columnFamily: Array[Byte]): Unit = {
    val tableToCreate = TableName.valueOf(tableName)

    if (!connection.getAdmin.isTableAvailable(tableToCreate)) {
      val builder = TableDescriptorBuilder.newBuilder(tableToCreate)

      val descriptor = new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(columnFamily)
      descriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL)
      builder.setColumnFamily(descriptor)

      connection.getAdmin.createTable(builder.build)
    }

  }


}

