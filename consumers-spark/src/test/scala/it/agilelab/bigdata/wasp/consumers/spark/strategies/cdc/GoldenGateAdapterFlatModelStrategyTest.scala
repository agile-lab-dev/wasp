package it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc

import it.agilelab.bigdata.wasp.consumers.spark.strategies.ReaderKey
import it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table.{OrderTable, OrderTableGoldenGate}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.avro.Schema
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, TryValues}

object GoldenGateAvroProvider {

  import com.typesafe.config.{Config, ConfigFactory}

  private def goldenGateAvroFormat(): String =
    """{
      |  "type" : "record",
      |  "name" : "OrderTable",
      |  "namespace" : "it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table",
      |  "fields" : [ {
      |    "name" : "table",
      |    "type" : "string"
      |  }, {
      |    "name" : "op_type",
      |    "type" : "string"
      |  }, {
      |    "name" : "op_ts",
      |    "type" : "string"
      |  }, {
      |    "name" : "current_ts",
      |    "type" : "string"
      |  }, {
      |    "name" : "pos",
      |    "type" : "string"
      |  }, {
      |    "name" : "primary_keys",
      |    "type" : {
      |      "type" : "array",
      |      "items" : "string"
      |    }
      |  }, {
      |    "name" : "tokens",
      |    "type" : {
      |      "type" : "map",
      |      "values" : "string"
      |    },
      |    "default" : { }
      |  }, {
      |    "name" : "CUST_CODE",
      |    "type" : [ "null", "string" ],
      |    "default" : null
      |  }, {
      |    "name" : "ORDER_DATE",
      |    "type" : [ "null", "string" ],
      |    "default" : null
      |  }, {
      |    "name" : "PRODUCT_CODE",
      |    "type" : [ "null", "string" ],
      |    "default" : null
      |  }, {
      |    "name" : "ORDER_ID",
      |    "type" : [ "null", "string" ],
      |    "default" : null
      |  }, {
      |    "name" : "PRODUCT_PRICE",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "PRODUCT_AMOUNT",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "TRANSACTION_ID",
      |    "type" : [ "null", "string" ],
      |    "default" : null
      |  } ]
      |}""".stripMargin

  def insertMessage(): String =
    """{"table": "it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table.OrderTable",
      |"op_type": "I",
      |"op_ts": "2013-06-02 22:14:36.000000",
      |"current_ts": "2015-09-18T10:13:11.172000",
      |"pos": "00000000000000001444",
      |"primary_keys": ["CUST_CODE", "ORDER_DATE", "PRODUCT_CODE", "ORDER_ID"],
      |"tokens": {"R": "AADPkvAAEAAEqL2AAA"},
      |"CUST_CODE": "WILL",
      |"ORDER_DATE": "1994-09-30:15:33:00",
      |"PRODUCT_CODE": "CAR",
      |"ORDER_ID": "144",
      |"PRODUCT_PRICE": 17520.0,
      |"PRODUCT_AMOUNT": 3.0,
      |"TRANSACTION_ID": "100"}""".stripMargin

  def updateMessage(): String =
    """{"table": "it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table.OrderTable",
      |"op_type": "U",
      |"op_ts": "2013-06-02 22:14:41.000000",
      |"current_ts": "2015-09-18T10:13:11.492000",
      |"pos": "00000000000000002891",
      |"primary_keys": ["CUST_CODE", "ORDER_DATE", "PRODUCT_CODE", "ORDER_ID"], "tokens":
      | {"R": "AADPkvAAEAAEqLzAAA"},
      |"CUST_CODE": "BILL",
      |"ORDER_DATE": "1995-12-31:15:00:00",
      |"PRODUCT_CODE": "CAR",
      |"ORDER_ID": "765",
      |"PRODUCT_PRICE": 14000.0,
      |"PRODUCT_AMOUNT": 3.0,
      |"TRANSACTION_ID": "100"}""".stripMargin

  def deleteMessage(): String =
    """{"table": "it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table.OrderTable",
      |"op_type": "D",
      |"op_ts": "2013-06-02 22:14:41.000000",
      |"current_ts": "2015-09-18T10:13:11.512000",
      |"pos": "00000000000000004338",
      |"primary_keys": ["CUST_CODE", "ORDER_DATE", "PRODUCT_CODE", "ORDER_ID"], "tokens":
      | {"L": "206080450", "6": "9.0.80330", "R": "AADPkvAAEAAEqLzAAC"}, "CUST_CODE":
      | "DAVE",
      |"ORDER_DATE": "1993-11-03:07:51:35",
      |"PRODUCT_CODE": "PLANE",
      |"ORDER_ID": "600",
      |"PRODUCT_PRICE": null,
      |"PRODUCT_AMOUNT": null,
      |"TRANSACTION_ID": null}""".stripMargin

  def truncateMessage(): String =
    """{"table": "it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table.OrderTable",
      |"op_type": "T",
      |"op_ts": "2013-06-02 22:14:41.000000",
      |"current_ts": "2015-09-18T10:13:11.514000",
      |"pos": "00000000000000004515",
      |"primary_keys": ["CUST_CODE", "ORDER_DATE", "PRODUCT_CODE", "ORDER_ID"], "tokens":
      | {"R": "AADPkvAAEAAEqL2AAB"},
      |"CUST_CODE": null,
      |"ORDER_DATE": null,
      |"PRODUCT_CODE": null,
      |"ORDER_ID": null,
      |"PRODUCT_PRICE": null,
      |"PRODUCT_AMOUNT": null,
      |"TRANSACTION_ID": null}""".stripMargin

  def getRowSchema: Schema = {
    val schema_obj = new Schema.Parser
    schema_obj.parse(goldenGateAvroFormat())
  }

  def primaryKeysConf: String = """goldengate.key.fields=["CUST_CODE", "ORDER_DATE", "PRODUCT_CODE", "ORDER_ID"]"""

  def strategyConfig: Config = ConfigFactory.parseString(primaryKeysConf)
}

class GoldenGateAdapterFlatModelStrategyTest
    extends FlatSpec
    with Matchers
    with SparkSuite
    with TryValues
    with BeforeAndAfter
    with Logging {

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
  import spark.implicits._

  implicit val jsonDefaultFormats: DefaultFormats.type = DefaultFormats
  val ggStrategy                                       = new GoldenGateAdapterFlatModelStrategy()
  ggStrategy.configuration = GoldenGateAvroProvider.strategyConfig

  behavior of "The GoldenGateStrategy"

  val expectedStruct: StructType = StructType(
    List(
      StructField(
        "key",
        StructType(
          List(
            StructField("CUST_CODE", StringType, nullable = true),
            StructField("ORDER_DATE", StringType, nullable = true),
            StructField("PRODUCT_CODE", StringType, nullable = true),
            StructField("ORDER_ID", StringType, nullable = true)
          )
        ),
        nullable = false
      ),
      StructField(
        "value",
        StructType(
          List(
            StructField(
              "beforeImage",
              StructType(
                List(
                  StructField("PRODUCT_AMOUNT", DoubleType, nullable = true),
                  StructField("TRANSACTION_ID", StringType, nullable = true),
                  StructField("ORDER_DATE", StringType, nullable = true),
                  StructField("PRODUCT_PRICE", DoubleType, nullable = true),
                  StructField("ORDER_ID", StringType, nullable = true),
                  StructField("CUST_CODE", StringType, nullable = true),
                  StructField("PRODUCT_CODE", StringType, nullable = true)
                )
              ),
              nullable = true
            ),
            StructField(
              "afterImage",
              StructType(
                List(
                  StructField("PRODUCT_AMOUNT", DoubleType, nullable = true),
                  StructField("TRANSACTION_ID", StringType, nullable = true),
                  StructField("ORDER_DATE", StringType, nullable = true),
                  StructField("PRODUCT_PRICE", DoubleType, nullable = true),
                  StructField("ORDER_ID", StringType, nullable = true),
                  StructField("CUST_CODE", StringType, nullable = true),
                  StructField("PRODUCT_CODE", StringType, nullable = true)
                )
              ),
              nullable = true
            ),
            StructField("type", StringType, nullable = false),
            StructField("timestamp", StringType, nullable = true),
            StructField("commitId", StringType, nullable = true)
          )
        ),
        nullable = false
      )
    )
  )

  def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit =
    dataframeA.collect().toSet shouldBe dataframeB.collect().toSet

  it should "fail if no configuration is provided" in {
    val insertObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.insertMessage()).extract[OrderTableGoldenGate]

    val df = Seq(insertObject).toDF()

    the[IllegalStateException] thrownBy (new GoldenGateAdapterFlatModelStrategy()
      .transform(Map((ReaderKey("input", "input"), df)))) should have message "the configuration goldengate.key.fields is not present, " +
      "cannot start the Strategy due unability to extract the primary key for the mutation"
  }

  it should "correctly fail if the dataframe doesn't contains the mandatory fields" in {
    val df = spark.emptyDataFrame

    import scala.util.{Failure, Try}

    Try {
      val ggStrategy = new GoldenGateAdapterFlatModelStrategy()
      ggStrategy.configuration = GoldenGateAvroProvider.strategyConfig
      ggStrategy.transform(Map((ReaderKey("input", "input"), df)))
    }.getClass shouldBe Failure(
      new IllegalStateException()
    ).getClass
  }

  it should "correctly translate the insert operation" in {
    val insertObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.insertMessage()).extract[OrderTableGoldenGate]

    val df = Seq(insertObject).toDF()

    val expectedResult =
      """{
        |   "key":{
        |      "CUST_CODE":"WILL",
        |      "ORDER_DATE":"1994-09-30:15:33:00",
        |      "PRODUCT_CODE":"CAR",
        |      "ORDER_ID":"144"
        |   },
        |   "value":{
        |      "afterImage":{
        |         "PRODUCT_AMOUNT":3.0,
        |         "TRANSACTION_ID":"100",
        |         "ORDER_DATE":"1994-09-30:15:33:00",
        |         "PRODUCT_PRICE":17520.0,
        |         "ORDER_ID":"144",
        |         "CUST_CODE":"WILL",
        |         "PRODUCT_CODE":"CAR"
        |      },
        |      "type":"insert",
        |      "timestamp":"2013-06-02 22:14:36.000000",
        |      "commitId":"00000000000000001444"
        |   }
        |}""".stripMargin

    val ggStrategy = new GoldenGateAdapterFlatModelStrategy()
    ggStrategy.configuration = GoldenGateAvroProvider.strategyConfig
    val res: DataFrame = ggStrategy.transform(Map((ReaderKey("input", "input"), df)))

    res.columns.toList shouldBe List("key", "value")
    res.schema shouldBe expectedStruct

    assertSameRows(spark.read.schema(res.schema).json(Seq(expectedResult).toDS()), res)
  }

  it should "correctly translate the update operation" in {
    val updateObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.updateMessage()).extract[OrderTableGoldenGate]

    val df = Seq(updateObject).toDF()

    val expectedResult =
      """{
        |   "key":{
        |      "CUST_CODE":"BILL",
        |      "ORDER_DATE":"1995-12-31:15:00:00",
        |      "PRODUCT_CODE":"CAR",
        |      "ORDER_ID":"765"
        |   },
        |   "value":{
        |      "afterImage":{
        |         "PRODUCT_AMOUNT":3.0,
        |         "TRANSACTION_ID":"100",
        |         "ORDER_DATE":"1995-12-31:15:00:00",
        |         "PRODUCT_PRICE":14000.0,
        |         "ORDER_ID":"765",
        |         "CUST_CODE":"BILL",
        |         "PRODUCT_CODE":"CAR"
        |      },
        |      "type":"update",
        |      "timestamp":"2013-06-02 22:14:41.000000",
        |      "commitId":"00000000000000002891"
        |   }
        |}""".stripMargin

    val ggStrategy = new GoldenGateAdapterFlatModelStrategy()
    ggStrategy.configuration = GoldenGateAvroProvider.strategyConfig
    val res: DataFrame = ggStrategy.transform(Map((ReaderKey("input", "input"), df)))

    res.columns.toList shouldBe List("key", "value")
    res.schema shouldBe expectedStruct

    assertSameRows(spark.read.schema(res.schema).json(Seq(expectedResult).toDS()), res)
  }

  it should "fail while translate the truncate operation" in {
    import scala.util.{Failure, Try}
    val truncateObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.truncateMessage()).extract[OrderTableGoldenGate]

    val df = Seq(truncateObject).toDF()

    Try {
      val ggStrategy = new GoldenGateAdapterFlatModelStrategy()
      ggStrategy.configuration = GoldenGateAvroProvider.strategyConfig
      ggStrategy.transform(Map((ReaderKey("input", "input"), df))).count()
    }.getClass shouldBe Failure(
      new IllegalStateException()
    ).getClass
  }

  it should "correctly translate more operation together" in {
    val insertObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.insertMessage()).extract[OrderTableGoldenGate]
    val updateObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.updateMessage()).extract[OrderTableGoldenGate]
    val deleteObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.deleteMessage()).extract[OrderTableGoldenGate]

    val df = Seq(insertObject, updateObject, deleteObject).toDF()

    val ggStrategy = new GoldenGateAdapterFlatModelStrategy()
    ggStrategy.configuration = GoldenGateAvroProvider.strategyConfig
    val res: DataFrame = ggStrategy.transform(Map((ReaderKey("input", "input"), df)))

    res.columns.toList shouldBe List("key", "value")

    res.schema shouldBe expectedStruct

    res.count() shouldBe 3
  }

  behavior of "The GoldenGateUtils"

  it should "translate a dataframe into an object table" in {

    val insertObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.insertMessage()).extract[OrderTableGoldenGate]
    val updateObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.updateMessage()).extract[OrderTableGoldenGate]
    val deleteObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.deleteMessage()).extract[OrderTableGoldenGate]

    val df = Seq(insertObject, updateObject, deleteObject).toDF()

    val innerTables: Set[OrderTable] = GoldenGateMutationUtils.extractTableDataset[OrderTable](df).collect().toSet

    Seq(insertObject, updateObject, deleteObject).map { m: OrderTableGoldenGate =>
      OrderTable(
        m.CUST_CODE,
        m.ORDER_DATE,
        m.PRODUCT_CODE,
        m.ORDER_ID,
        m.PRODUCT_PRICE,
        m.PRODUCT_AMOUNT,
        m.TRANSACTION_ID
      )
    }.toSet shouldBe innerTables
  }

  it should "translate a dataframe into a monad called Mutation table that contains the inner table" in {

    val insertObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.insertMessage()).extract[OrderTableGoldenGate]
    val updateObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.updateMessage()).extract[OrderTableGoldenGate]
    val deleteObject: OrderTableGoldenGate =
      JsonMethods.parse(GoldenGateAvroProvider.deleteMessage()).extract[OrderTableGoldenGate]

    val df = Seq(insertObject, updateObject, deleteObject).toDF()

    val extracted: Set[TableMutationFlatModel[OrderTable]] =
      GoldenGateMutationUtils.mapIntoCaseClass[OrderTable](df).collect().toSet

    extracted.size shouldBe 3

    import spark.implicits._

    val innerTables: Set[OrderTable] = GoldenGateMutationUtils.extractTableDataset[OrderTable](df).collect().toSet

    extracted.map(p => p.innerTable) shouldBe innerTables
  }

}
