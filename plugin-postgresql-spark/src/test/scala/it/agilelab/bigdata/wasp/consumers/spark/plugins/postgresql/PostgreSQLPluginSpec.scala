package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.PostgreSQLProduct
import it.agilelab.bigdata.wasp.models._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class PostgreSQLPluginSpec extends FlatSpec with Matchers with BeforeAndAfterAll with SparkSuite {
  import PostgreSQLPluginSpec._

  private lazy val plugin: PostgreSQLConsumerSparkPlugin = instantiatePlugin

  it should "error out saying that Spark Structured Streaming Reader is unsupported" in {
    an[IllegalArgumentException] should be thrownBy plugin.getSparkStructuredStreamingReader(
      null,
      dummyStructuredStreamingETLModel,
      dummyStreamingReaderModel
    )
  }

  it should "error out saying that Spark Batch Reader is unsupported" in {
    an[IllegalArgumentException] should be thrownBy plugin.getSparkBatchReader(null, dummyReaderModel)
  }

  it should "error out saying that Spark Batch Reader is not implemented" in {
    an[IllegalArgumentException] should be thrownBy plugin.getSparkBatchWriter(null, dummyWriterModel)
  }

  private def instantiatePlugin: PostgreSQLConsumerSparkPlugin = {
    val plugin = new PostgreSQLConsumerSparkPlugin
    plugin.initialize(null) // we don't use waspDB in the initialize so it' fine for it to be null
    plugin
  }
}

object PostgreSQLPluginSpec {
  protected var dummysqlSinkModel: SQLSinkModel = SQLSinkModel(
    name = "dummy",
    table = "dummy",
    tableAliasForExistingValues = "dummyAlias",
    primaryKeys = List("pk1"),
    writeMode = UpsertIgnoreExisting,
    updateClauses = None,
    PostgreSQL,
    jdbcConnection =
      JDBCConnection(name = "dummy", url = "", user = "", password = "", driverName = "", properties = None),
    batchSize = 10,
    poolSize = 4
  )
  protected val dummyWriterModel = WriterModel.apply("dummy", dummysqlSinkModel, PostgreSQLProduct)
  protected val dummyReaderModel = ReaderModel.apply("dummy", dummysqlSinkModel, PostgreSQLProduct)
  protected val dummyStreamingReaderModel =
    StreamingReaderModel.apply("dummy", dummysqlSinkModel, PostgreSQLProduct, None)
  protected val dummyStructuredStreamingETLModel = StructuredStreamingETLModel(
    name = "dummy",
    streamingInput = dummyStreamingReaderModel,
    staticInputs = List(dummyReaderModel),
    streamingOutput = dummyWriterModel,
    mlModels = List.empty[MlModelOnlyInfo],
    strategy = None,
    triggerIntervalMs = None
  )
}
