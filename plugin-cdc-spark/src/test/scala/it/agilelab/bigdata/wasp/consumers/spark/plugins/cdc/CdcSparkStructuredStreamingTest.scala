package it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc

import io.delta.tables.DeltaTable
import it.agilelab.bigdata.wasp.consumers.spark.eventengine.SparkSetup
import it.agilelab.bigdata.wasp.models.{CdcModel, CdcOptions}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.WordSpec


class CdcSparkStructuredStreamingTest extends WordSpec with TempDirectoryEach with SparkSetup {

  val cdcTestModel: CdcModel = CdcModel(
    name = "TestMutationSchemaModel",
    uri = tempDir,
    schema = StructType(Seq(
      StructField("A", StringType),
      StructField("A2", StringType),
      StructField("B", StringType),
      StructField("B2", StringType)
    )).json,
    options = CdcOptions.defaultAppend)

  val mutationSchema = StructType(Seq(
    StructField("key", StructType(Seq(
      StructField("A", StringType),
      StructField("B", StringType)
    ))),
    StructField("value", StructType(Seq(
      StructField("timestamp", StringType),
      StructField("commitId", StringType),
      StructField("beforeImage", StructType(Seq(
        StructField("A", StringType),
        StructField("A2", StringType),
        StructField("B", StringType),
        StructField("B2", StringType)
      ))),
      StructField("afterImage", StructType(Seq(
        StructField("A", StringType),
        StructField("A2", StringType),
        StructField("B", StringType),
        StructField("B2", StringType)
      ))),
      StructField("type", StringType)
    )))
  ))


  val keys = Seq("A", "B")
  val mutation1 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10000", "commitId":"0", "beforeImage":null, "afterImage": null, "type":"insert"}}"""
  val mutation2 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10010", "commitId":"1", "beforeImage":null, "afterImage": {"A":"x", "A2":"yy", "B":"z", "B2":"xx"}, "type":"insert"}}"""
  val mutation3 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10020", "commitId":"2", "beforeImage":null, "afterImage": {"A":"x", "A2":"yy", "B":"z", "B2":"xx"}, "type":"insert"}}"""
  val mutation4 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10050", "commitId":"3", "beforeImage":null, "afterImage": {"A":"a", "A2":"yy", "B":"b", "B2":"xx"}, "type":"insert"}}"""
  val mutation5 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10060", "commitId":"4", "beforeImage":null, "afterImage": {"A":"a", "A2":"qq", "B":"b", "B2":"qq"}, "type":"insert"}}"""
  //---------------------
  val mutation6 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10070", "commitId":"1", "beforeImage": {"A":"x", "A2":"yy", "B":"z", "B2":"xx"}, "afterImage":null, "type":"delete"}}"""
  val mutation7 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10080", "commitId":"2", "beforeImage": {"A":"x", "A2":"yy", "B":"z", "B2":"xx"}, "afterImage":null, "type":"delete"}}"""
  val mutation8 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10090", "commitId":"3", "beforeImage": {"A":"a", "A2":"yy", "B":"b", "B2":"xx"}, "afterImage":null, "type":"delete"}}"""
  val mutation9 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10100", "commitId":"4", "beforeImage": {"A":"a", "A2":"qq", "B":"b", "B2":"qq"}, "afterImage":null, "type":"delete"}}"""
  val mutation10 =  """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10110", "commitId":"1", "beforeImage": {"A":"x", "A2":"yy", "B":"z", "B2":"xx"}, "afterImage":null, "type":"delete"}}"""
  //---------------------
  val mutation11 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10010", "commitId":"1", "beforeImage":{"A":"x", "A2":"aa", "B":"z", "B2":"bb"}, "afterImage": {"A":"x", "A2":"yy", "B":"z", "B2":"xx"}, "type":"update"}}"""
  val mutation12 =   """{"key": {"A":"x","B":"z"}, "value": {"timestamp":"10011", "commitId":"1", "beforeImage":{"A":"x", "A2":"hh", "B":"z", "B2":"hh"}, "afterImage": {"A":"x", "A2":"aa", "B":"z", "B2":"bb"}, "type":"update"}}"""
  val mutation13 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10012", "commitId":"1", "beforeImage":null, "afterImage": {"A":"a", "A2":"ww", "B":"b", "B2":"qq"}, "type":"update"}}"""
  //---------------------
  val mutation15 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10010", "commitId":"1", "beforeImage":null, "afterImage": {"A":"a", "A2":"aa", "B":"b", "B2":"bb"}, "type":"insert"}}"""
  val mutation16 =   """{"key": {"A":"c","B":"d"}, "value": {"timestamp":"10011", "commitId":"1", "beforeImage":null, "afterImage": {"A":"c", "A2":"ac", "B":"d", "B2":"bd"}, "type":"update"}}"""
  val mutation17 =   """{"key": {"A":"a","B":"b"}, "value": {"timestamp":"10012", "commitId":"1", "beforeImage":{"A":"a", "A2":"aa", "B":"b", "B2":"bb"}, "afterImage": {"A":"a", "A2":"xx", "B":"b", "B2":"xx"}, "type":"update"}}"""
  val mutation18 =   """{"key": {"A":"c","B":"d"}, "value": {"timestamp":"10013", "commitId":"1", "beforeImage":{"A":"c", "A2":"ac", "B":"d", "B2":"bd"}, "afterImage":null, "type":"delete"}}"""
  val mutation19 =   """{"key": {"A":"e","B":"f"}, "value": {"timestamp":"10014", "commitId":"1", "beforeImage":null, "afterImage":{"A":"e", "A2":"ee", "B":"f", "B2":"ff"}, "type":"insert"}}"""
  val mutation20 =   """{"key": {"A":"h","B":"j"}, "value": {"timestamp":"10015", "commitId":"1", "beforeImage":{"A":"c", "A2":"ac", "B":"d", "B2":"bd"}, "afterImage":null, "type":"delete"}}"""

  def assertSameRows(dataframeA: DataFrame, dataframeB: DataFrame): Unit =
    assert(dataframeA.collect().toSet == dataframeB.collect().toSet)


  "Method 'initDeltaTable'" should {

    "correctly initialize an empty delta table at the given path with the given schema." in withSparkSession { ss => {

      DeltaLakeOperations.initDeltaTable(cdcTestModel.uri, DataType.fromJson(cdcTestModel.schema).asInstanceOf[StructType], ss)

      assert(DeltaTable.isDeltaTable(cdcTestModel.uri))

      val readDF = ss.read.format("delta").load(cdcTestModel.uri)
      assert(readDF.isEmpty)

      val readDF_fields = readDF.schema.fields
      val expected_fields = DataType.fromJson(cdcTestModel.schema).asInstanceOf[StructType].fields

      assert(readDF_fields.diff(expected_fields).isEmpty && readDF_fields.length == expected_fields.length)

    }
    }
  }


  "Method 'getLatestChangeForKey'" should {
    "correctly filter out only the most recent mutations." in withSparkSession { ss => {

      val deltaLakeWriter: DeltaLakeWriter = new DeltaLakeWriter(cdcTestModel, ss)
      import ss.implicits._

      val df = ss.read.schema(mutationSchema).json(Seq(mutation1, mutation2, mutation3, mutation4, mutation5).toDS)
      val compactDf = deltaLakeWriter.getLatestChangeForKey(df, keys)

      compactDf.printSchema()

      val keyColumns: String = keys
        .map(keyField => s"coalesce(value.beforeImage.$keyField, value.afterImage.$keyField) as $keyField")
        .mkString(",")

      val expectedDf = ss.read.schema(mutationSchema).json(Seq(mutation3, mutation5).toDS)
        .selectExpr(Seq("value.timestamp as _timestamp", "value.commitId as _commitId", "value.type as _type", "coalesce(value.afterImage, value.beforeImage) as _value", s"struct($keyColumns)"): _*)

      assertSameRows(compactDf, expectedDf)
    }
    }

    "correctly work even with a single row dataset." in withSparkSession { ss => {
      val deltaLakeWriter: DeltaLakeWriter = new DeltaLakeWriter(cdcTestModel, ss)
      import ss.implicits._
      val df2 = ss.read.schema(mutationSchema).json(Seq(mutation1).toDS)
      val compactDf2 = deltaLakeWriter.getLatestChangeForKey(df2, keys)

      val keyColumns: String = keys
        .map(keyField => s"coalesce(value.beforeImage.$keyField, value.afterImage.$keyField) as $keyField")
        .mkString(",")

      val expectedDf2 = ss.read.schema(mutationSchema).json(Seq(mutation1).toDS)
        .selectExpr(Seq("value.timestamp as _timestamp", "value.commitId as _commitId", "value.type as _type", "coalesce(value.afterImage, value.beforeImage) as _value", s"struct($keyColumns)"): _*)
      assertSameRows(compactDf2, expectedDf2)

    }
    }
  }

  "Method 'write'" should {
    "correctly execute the merge operation into the delta table with mutations only containing 'insert' operations." in withSparkSession { ss => {
      val deltaLakeWriter: DeltaLakeWriter = new DeltaLakeWriter(cdcTestModel, ss)
      import ss.implicits._

      val stringDf = Seq(mutation2, mutation3, mutation4, mutation5).toDS
      val toMergeDf = ss.read.schema(mutationSchema).json(stringDf)

      val expected3 = """{"A":"x", "A2":"yy", "B":"z", "B2":"xx"}"""
      val expected5 = """{"A":"a", "A2":"qq", "B":"b", "B2":"qq"}"""
      val expectedStringDf = Seq(expected3, expected5).toDS
      val expectedDf = ss.read.json(expectedStringDf)

      deltaLakeWriter.write(toMergeDf, 0)

      val actualDf = ss.read.format(cdcTestModel.options.format).load(cdcTestModel.uri)

      assertSameRows(expectedDf, actualDf)
    }}

    "not write anything into an empty delta table with mutation only containing 'delete' operations. " in withSparkSession { ss => {
      val deltaLakeWriter: DeltaLakeWriter = new DeltaLakeWriter(cdcTestModel, ss)
      import ss.implicits._

      val toMergeDf = ss.read.schema(mutationSchema).json(Seq(mutation6, mutation7, mutation8, mutation9, mutation10).toDS)
      val expectedDf = ss.createDataFrame(ss.sparkContext.emptyRDD[Row], DataType.fromJson(cdcTestModel.schema).asInstanceOf[StructType])

      deltaLakeWriter.write(toMergeDf, 0)
      val actualDf = ss.read.format(cdcTestModel.options.format).load(cdcTestModel.uri)
      assertSameRows(expectedDf, actualDf)
    }}

    "correctly execute the merge operation into the delta table with mutations only containg 'update' operations and with null value in after/beforeImage." in withSparkSession { ss => {
      val deltaLakeWriter: DeltaLakeWriter = new DeltaLakeWriter(cdcTestModel, ss)
      import ss.implicits._

      val toMergeDf = ss.read.schema(mutationSchema).json(Seq(mutation11, mutation12, mutation13).toDS)

      val expectedJson1 = """{"A":"x", "A2":"aa", "B":"z", "B2":"bb"}"""
      val expectedJson2 = """{"A":"a", "A2":"ww", "B":"b", "B2":"qq"}"""
      val expectedDf = ss.read.json(Seq(expectedJson1, expectedJson2).toDS)

      deltaLakeWriter.write(toMergeDf, 0)
      val actualDf = ss.read.format(cdcTestModel.options.format).load(cdcTestModel.uri)
      assertSameRows(expectedDf, actualDf)
    }}

    "correctly execute the merge operation into the delta table with mutations containing all the three operations." in withSparkSession { ss => {
      val deltaLakeWriter: DeltaLakeWriter = new DeltaLakeWriter(cdcTestModel, ss)
      import ss.implicits._

      val toMergeDf = ss.read.schema(mutationSchema).json(Seq(mutation15, mutation16, mutation17, mutation18, mutation19, mutation20).toDS)

      val expectedJson1 = """{"A":"a", "A2":"xx", "B":"b", "B2":"xx"}"""
      val expectedJson2 = """{"A":"e", "A2":"ee", "B":"f", "B2":"ff"}"""
      val expectedDf = ss.read.json(Seq(expectedJson1, expectedJson2).toDS)

      deltaLakeWriter.write(toMergeDf, 0)
      val actualDf = ss.read.format(cdcTestModel.options.format).load(cdcTestModel.uri)
      assertSameRows(expectedDf, actualDf)
    }}

  }

}
