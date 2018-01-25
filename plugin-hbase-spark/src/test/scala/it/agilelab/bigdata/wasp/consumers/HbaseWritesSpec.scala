package it.agilelab.bigdata.wasp.consumers
/*


import org.apache.hadoop.hbase.spark.LatestHBaseContextCache
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.hadoop.hbase.spark.example.datasources.AvroHBaseRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, _}

/**
  * Created by mattiabertorello on 27/01/17.
  */

case class Avro(
                 name: String,
                 favorite_number: Int,
                 favorite_color: String,
                 favorite_array: Array[String],
                 favorite_map: Map[String, Int]
               )

case class HBaseRecord(
                        col0: String,
                        col1: Boolean,
                        col2: Double,
                        col3: Float,
                        col4: Int,
                        col5: Long,
                        col6: Short,
                        col7: String,
                        col8: Byte,
                        col9: Avro
                      )

object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte,
      Avro(
        "avro",
        i,
        "color1",
        Array("color1"),
        Map("k1" -> 1)
      )
    )
  }
}

class HbaseWritesSpec  extends FlatSpec  with ScalaFutures with BeforeAndAfter   {
  val schemaString =
    s"""{"namespace": "example.avro",
       |   "type": "record", "name": "User",
       |    "fields": [
       |        {"name": "name", "type": "string"},
       |        {"name": "favorite_number",  "type": ["int", "null"]},
       |        {"name": "favorite_color", "type": ["string", "null"]},
       |        {"name": "favorite_array", "type": {"type": "array", "items": "string"}},
       |        {"name": "favorite_map", "type": {"type": "map", "values": "int"}}
       |      ]
       |}""".stripMargin


  val cat =
    s"""{
       |"table":{"namespace":"default", "name":"HBaseSourceExampleTable"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
       |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
       |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
       |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
       |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
       |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
       |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"},
       |"col9":{"cf":"cf1", "col":"col9", "avro":"avroSchema"}
       |}
       |}""".stripMargin


  behavior of "HBase writer"

  it should "convert the json schema" in {

    val ss = SparkSession.builder.config(new SparkConf().setMaster("local").setAppName("hbasetest")).getOrCreate()

        val data = (0 to 255).map { i =>
          HBaseRecord(i)
        }

        ss.createDataFrame(data).write.options(
          Map(HBaseTableCatalog.tableCatalog -> cat,
            "avroSchema" -> schemaString,
            HBaseSparkConf.HBASE_CONFIG_LOCATION -> "/wasp/docker/hbase/conf_ex/hbase-site.xml",
            HBaseTableCatalog.newTable -> "5",
            HBaseSparkConf.USE_HBASECONTEXT -> "false"))
          .format("org.apache.hadoop.hbase.spark")
          .save()

    val dataRead = ss
      .read
      .options(Map("avroSchema" -> schemaString,
        HBaseTableCatalog.tableCatalog -> cat,
        HBaseSparkConf.HBASE_CONFIG_LOCATION -> "/wasp/docker/hbase/conf_ex/hbase-site.xml"
      ))
      .format("org.apache.hadoop.hbase.spark")
      .load()
    dataRead.show(100, false)
    println("End normal writing")
    import ss.implicits._
    val input = new MemoryStream[HBaseRecord](42, ss.sqlContext)
    input.addData(
      Seq(
        HBaseRecord(302),
        HBaseRecord(301),
        HBaseRecord(300)
      ))
    val s = input.toDF().writeStream.options(
      Map(HBaseTableCatalog.tableCatalog -> cat,
        "avroSchema" -> schemaString,
        HBaseSparkConf.HBASE_CONFIG_LOCATION -> "/wasp/docker/hbase/conf_ex/hbase-site.xml",
        HBaseTableCatalog.newTable -> "5"))
      .option("checkpointLocation", "/tmp/plugin-hbase-wasp")
      .format("org.apache.hadoop.hbase.spark")
      .start()
    s.awaitTermination(30000)
    s.stop()

    input.addData(
      Seq(
        HBaseRecord(304),
        HBaseRecord(305),
        HBaseRecord(306)
      ))

    val s1 = input.toDF().writeStream.options(
      Map(HBaseTableCatalog.tableCatalog -> cat,
        "avroSchema" -> schemaString,
        HBaseSparkConf.HBASE_CONFIG_LOCATION -> "/wasp/docker/hbase/conf_ex/hbase-site.xml",
        HBaseTableCatalog.newTable -> "5"))
      .option("checkpointLocation", "/tmp/plugin-hbase-wasp")
      .format("org.apache.hadoop.hbase.spark")
      .start()
    s1.awaitTermination(30000)
    s1.stop()
    LatestHBaseContextCache.latest.close()

/*
    val r: HbaseTableModel = HBaseWriter.getHbaseConfDataConvert(
      """
        |{
        |  "table": {
        |    "namespace": "default",
        |    "name": "htable"
        |  },
        |  "rowkey": [
        |    {
        |      "col": "jsonCol2",
        |      "type": "string"
        |    }
        |  ],
        |  "columns": {
        |    "cf1": [
        |      {
        |        "HBcol1": {
        |          "col": "jsonCol3",
        |          "type": "string",
        |          "mappingType": "oneToOne"
        |        }
        |      }
        |     ]
        |  }
        |}
      """.stripMargin)

    r.rowKey should be(Seq(RowKeyInfo("jsonCol2", "string")))
    r.table should be(TableNameC("default", "htable"))
    r.columns should be(Map("cf1"-> List(Map("HBcol1" -> InfoCol(Some("jsonCol3"), Some("string"), "oneToOne", None, None)))))
  }

  it should "write on hbase" in {
    val hbaseModel: KeyValueModel = KeyValueModel("test",
      schema ="""
        |{"table":{"namespace":"vodafone.table", "name":"rw_trip_hz"},
        |   "rowkey":[
        |       { "col": "col_df", "type" : "string"}
        |    ],
        |    "columns": {
        |    "cf1": [
        |      {
        |        "HBcol1": {
        |          "col": "col_df",
        |          "type": "string",
        |          "mappingType": "oneToOne"
        |        }
        |      }
        |     ]
        |  }
        |}
        |
      """.stripMargin,
      dataFrameSchema =
        """
          |{
          |  "type":"struct",
          |       "fields":[
          |             {"name":"col_df","type":"integer", "nullable": false, "metadata":{}}
          |         ]
          |}
        """.stripMargin,
      avroSchemas= Some(Map())
    )
    val schema: StructType = DataType.fromJson(hbaseModel.dataFrameSchema).asInstanceOf[StructType]

    val rowAvroConverters: Map[String, RowToAvro] = hbaseModel.avroSchemas.map(_.mapValues(v => {
      new RowToAvro(schema, v)
    })).getOrElse(Map[String, RowToAvro]())

    val hbaseDataConfig = HBaseWriter.getHbaseConfDataConvert(hbaseModel.schema)
    val funcConverted = HBaseWriter.getConvertPutFunc(hbaseDataConfig, rowAvroConverters)

    val row = new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(Array("testValue"),schema)
    val put: Put = funcConverted(row)

    //put.toMap().get("row").toString shouldBe "testValue"

    put.get(Bytes.toBytes("cf1"), Bytes.toBytes("HBcol1")).size() shouldBe 1
    //Bytes.toString(put.get(Bytes.toBytes("cf1"), Bytes.toBytes("HBcol1")).get(0).getValueArray) shouldBe "testValue"

  }

  it should "write on hbase with avro" in {
    val avroSchema = s"""
                                        |{"namespace": "vodafone.avro",
                                        |           "type": "record",
                                        |           "name": "schema_trip",
                                        |           "fields": [
                                        |             {"name":"col_df", "type":"string"},
                                        |             {"name":"col_ts", "type":"int"}
                                        |           ]
                                        |          }
       """.stripMargin

    val hbaseModel: KeyValueModel = KeyValueModel("test",
      schema ="""
                |{"table":{"namespace":"vodafone.table", "name":"rw_trip_hz"},
                |   "rowkey":[
                |       { "col": "col_df", "type" : "string"}
                |    ],
                |    "columns": {
                |    "cf1": [
                |      {
                |        "HBcol1": {
                |          "col": "col_df",
                |          "type": "string",
                |          "mappingType": "oneToOne"
                |        }
                |      },
                |      {
                |        "HBcol2": {
                |          "avro" : "schema_trip",
                |          "pivotCol": "col_ts",
                |          "mappingType": "oneToMany"
                |        }
                |      }
                |     ]
                |  }
                |}
                |
      """.stripMargin,
      dataFrameSchema =
        """
          |{
          |  "type":"struct",
          |       "fields":[
          |             {"name":"col_df","type":"string", "nullable": false, "metadata":{}},
          |             {"name":"col_ts","type":"integer", "nullable": false, "metadata":{}}
          |         ]
          |}
        """.stripMargin,
      avroSchemas= Some(Map("schema_trip" -> avroSchema))
    )
    val schema: StructType = DataType.fromJson(hbaseModel.dataFrameSchema).asInstanceOf[StructType]

    val rowAvroConverters: Map[String, RowToAvro] = hbaseModel.avroSchemas.map(_.mapValues(v => {
      new RowToAvro(schema, v)
    })).getOrElse(Map[String, RowToAvro]())

    val hbaseDataConfig = HBaseWriter.getHbaseConfDataConvert(hbaseModel.schema)
    val funcConverted = HBaseWriter.getConvertPutFunc(hbaseDataConfig, rowAvroConverters)

    val row = new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(Array("testValue2", 10),schema)
    val put: Put = funcConverted(row)

    //put.toMap().get("row").toString shouldBe "testValue"

    put.get(Bytes.toBytes("cf1"), Bytes.toBytes("HBcol1")).size() shouldBe 1
    //Bytes.toString(put.get(Bytes.toBytes("cf1"), Bytes.toBytes("HBcol1")).get(0).getValueArray) shouldBe "testValue"
    put.get(Bytes.toBytes("cf1"), Bytes.toBytes("HBcol210")).get(0).getValue.length > 0 shouldBe true
    AvroToJsonUtil.avroToJson(put.get(Bytes.toBytes("cf1"), Bytes.toBytes("HBcol210")).get(0).getValue) shouldBe "{\"col_df\":\"testValue2\",\"col_ts\":10}"
*/
  }



}
*/
