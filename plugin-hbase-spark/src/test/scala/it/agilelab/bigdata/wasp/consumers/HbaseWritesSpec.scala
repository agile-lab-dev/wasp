package it.agilelab.bigdata.wasp.consumers

import it.agilelab.bigdata.wasp.consumers.writers._
import it.agilelab.bigdata.wasp.core.models.KeyValueModel
import it.agilelab.bigdata.wasp.core.utils.{AvroToJsonUtil, RowToAvro}
import org.apache.avro.Schema
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, _}

/**
  * Created by mattiabertorello on 27/01/17.
  */
/*
case class DataTestClass(d1s: String, d2ts: Long)

class HbaseWritesSpec  extends FlatSpec  with ScalaFutures with BeforeAndAfter   {

    behavior of "HBase writer"

  it should "convert the json schema" in {

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

  }



}
*/