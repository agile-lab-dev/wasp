package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql.PostgresSuite._
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models._
import org.apache.spark.sql.{Encoder, SparkSession}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.sql.{Connection, Timestamp}

/** Tests for [[PostgreSQLSparkBatchWriter]].
  */
class PostgreSQLBatchWriterSpec
    extends FunSuite
    with SparkSuite
    with PostgresSuite
    with PostgresTestSupport
    with Logging {

  test("Upsert update existing") {
    import spark.implicits._

    val testName = "upsertUpdateExistingBatch"
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
    writeBatch(sqlSinkModel, input1, spark)
    val input2: Seq[TestData] =
      (1 to 5).map(x => TestData(x.toString, x, "hello", x, new Timestamp(System.currentTimeMillis())))
    writeBatch(sqlSinkModel, input2, spark)

    printTableContents(testName, connection)

    getTableContents(testName, connection) should contain theSameElementsAs squash(input1 ++ input2)
  }

  test("Upsert ignore existing") {
    import spark.implicits._

    val testName = "upsertIgnoreExistingBatch"

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
    writeBatch(sqlSinkModel, input1, spark)
    val input2: Seq[TestData] =
      (1 to 5).map(x => TestData(x.toString, x, "hello", x, new Timestamp(System.currentTimeMillis())))
    writeBatch(sqlSinkModel, input2, spark)

    printTableContents(testName, connection)

    getTableContents(testName, connection) should contain theSameElementsAs squash(input1)
  }

  test("Is idempotent") {
    import spark.implicits._

    val testName = "idempotentBatch"
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
      writeBatch(sqlSinkModel, input1, spark)
      writeBatch(sqlSinkModel, input2, spark)
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

  private def writeBatch[A: Encoder](sqlSinkModel: SQLSinkModel, inputData: Seq[A], spark: SparkSession): Unit = {
    val df     = spark.createDataset(inputData).repartition(4).toDF()
    val writer = new PostgreSQLSparkBatchWriter(sqlSinkModel)

    writer.write(df)
  }

}
