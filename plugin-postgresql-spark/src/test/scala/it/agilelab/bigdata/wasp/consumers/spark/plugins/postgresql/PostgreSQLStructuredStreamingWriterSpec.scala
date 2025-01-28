package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql.PostgresSuite._
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.{
  JDBCConnection,
  PostgreSQL,
  SQLSinkModel,
  UpsertIgnoreExisting,
  UpsertUpdateExisting
}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{Encoder, SparkSession}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.sql.{Connection, Timestamp}
import java.util.UUID

/**
  * Tests for [[PostgreSQLSparkStructuredStreamingWriter]].
  */
class PostgreSQLStructuredStreamingWriterSpec
    extends FunSuite
    with SparkSuite
    with PostgresSuite
    with PostgresTestSupport
    with Logging {

  test("Upsert update existing") {
    import spark.implicits._

    val testName = "upsertUpdateExistingStreaming"
    val updateClauses = Map(
      ("val1", "case when existing.val1 is null then excluded.val1 else existing.val1 end"),
      ("val2", "case when existing.val2 is null then excluded.val2 else least(existing.val2, excluded.val2) end"),
      ("val3", "case when existing.val3 is null then excluded.val3 else greatest(existing.val3, excluded.val3) end")
    )

    val sqlSinkModel =
      SQLSinkModel(
        testName,
        testName,
        "existing",
        List("pk1", "pk2"),
        UpsertUpdateExisting,
        Some(updateClauses),
        PostgreSQL,
        JDBCConnection(testName, jdbcUrl, user, password, driver, properties = None),
        10,
        4
      )

    createTableForTestData(testName, connection)

    val input1: Seq[TestData] =
      (1 to 10).map(x => TestData(x.toString, x, null, Long.MaxValue, new Timestamp(System.currentTimeMillis())))
    createAndExecuteStreamingQuery(sqlSinkModel, input1, spark)
    val input2: Seq[TestData] =
      (1 to 5).map(x => TestData(x.toString, x, "hello", x, new Timestamp(System.currentTimeMillis())))
    createAndExecuteStreamingQuery(sqlSinkModel, input2, spark)

    printTableContents(testName, connection)

    getTableContents(testName, connection) should contain theSameElementsAs squash(input1 ++ input2)
  }

  test("Upsert ignore existing") {
    import spark.implicits._

    val testName = "upsertIgnoreExistingStreaming"

    val sqlSinkModel =
      SQLSinkModel(
        testName,
        testName,
        "existing",
        List("pk1", "pk2"),
        UpsertIgnoreExisting,
        None,
        PostgreSQL,
        JDBCConnection(testName, jdbcUrl, user, password, driver, properties = None),
        10,
        4
      )

    createTableForTestData(testName, connection)

    val input1: Seq[TestData] =
      (1 to 10).map(x => TestData(x.toString, x, null, Long.MaxValue, new Timestamp(System.currentTimeMillis())))
    createAndExecuteStreamingQuery(sqlSinkModel, input1, spark)
    val input2: Seq[TestData] =
      (1 to 5).map(x => TestData(x.toString, x, "hello", x, new Timestamp(System.currentTimeMillis())))
    createAndExecuteStreamingQuery(sqlSinkModel, input2, spark)

    printTableContents(testName, connection)

    getTableContents(testName, connection) should contain theSameElementsAs squash(input1)
  }

  test("Is idempotent") {
    import spark.implicits._

    val testName = "idempotentStreaming"
    val updateClauses = Map(
      ("val1", "case when existing.val1 is null then excluded.val1 else existing.val1 end"),
      ("val2", "case when existing.val2 is null then excluded.val2 else least(existing.val2, excluded.val2) end"),
      ("val3", "case when existing.val3 is null then excluded.val3 else greatest(existing.val3, excluded.val3) end")
    )

    val sqlSinkModel =
      SQLSinkModel(
        testName,
        testName,
        "existing",
        List("pk1", "pk2"),
        UpsertUpdateExisting,
        Some(updateClauses),
        PostgreSQL,
        JDBCConnection(testName, jdbcUrl, user, password, driver, properties = None),
        10,
        4
      )

    createTableForTestData(testName, connection)

    val input1: Seq[TestData] =
      (1 to 10).map(x => TestData(x.toString, x, null, Long.MaxValue, new Timestamp(System.currentTimeMillis())))
    val input2: Seq[TestData] =
      (1 to 5).map(x => TestData(x.toString, x, "hello", x, new Timestamp(System.currentTimeMillis())))
    (1 to 3).foreach { _ =>
      createAndExecuteStreamingQuery(sqlSinkModel, input1, spark)
      createAndExecuteStreamingQuery(sqlSinkModel, input2, spark)
    }

    printTableContents(testName, connection)

    getTableContents(testName, connection) should contain theSameElementsAs squash(input1 ++ input2)
  }

  private def createTableForTestData(tableName: String, connection: Connection): Unit = {
    val cts = connection.createStatement()
    cts.execute(s"""CREATE TABLE ${tableName.toUpperCase} (
         |    pk1     VARCHAR(32),
         |    pk2     INTEGER,
         |    val1    VARCHAR(32),
         |    val2    BIGINT,
         |    val3    TIMESTAMP,
         |    PRIMARY KEY (pk1, pk2)
         |)
         |""".stripMargin)
    cts.close()
  }

  private def createAndExecuteStreamingQuery[A: Encoder](
      sqlSinkModel: SQLSinkModel,
      inputData: Seq[A],
      spark: SparkSession
  ): Option[StreamingQueryException] = {
    val memoryStream = new MemoryStream[A](0, spark.sqlContext)
    val df           = memoryStream.toDF().repartition(4)
    val dsw          = new PostgreSQLSparkStructuredStreamingWriter(sqlSinkModel).write(df)
    val query = dsw
      .option("checkpointLocation", s"/tmp/PostgreSQLStructuredStreamingWriterSpec/${UUID.randomUUID().toString}")
      .start()

    memoryStream.addData(inputData: _*)
    query.processAllAvailable()
    query.stop()
    query.awaitTermination()
    query.exception
  }

}
