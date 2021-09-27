package org.apache.spark.sql.datasources.hbase

import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.models.builder.KVColumnFamily.ColumnFamilyBuilder
import it.agilelab.bigdata.wasp.models.builder.{KVColumn, KVSchemaBuilder, KVType}
import org.scalatest.FunSuite

import scala.collection.mutable

class HBaseTableCatalogTest extends FunSuite {
  test("1") {
    val avroSchema = """{
                       |  "type": "record",
                       |  "name": "LongList",
                       |  "aliases": ["LinkedLongs"],
                       |  "fields" : [
                       |    {"name": "value", "type": "long"}
                       |  ]
                       |}""".stripMargin
    val kvSchema = KVSchemaBuilder.emptySchemaBuilder
      .withKey("rowkey" -> KVType.STRING)
      .withFamily(
        ColumnFamilyBuilder
          .withName("myCf")
          .withCellQualifier(KVColumn.primitive("field", "qualifier", KVType.LONG))
          .withCellQualifier(KVColumn.avro("structField", "q2", "avroSchema"))
          .withCellQualifier(KVColumn.primitive("timestamp", "timestamp", KVType.STRING))
          .withCellQualifier(KVColumn.primitive("timestamp2", "timestamp2", KVType.STRING))
          .withClusteringColumn(KVColumn.primitive("ts", "timestamp", KVType.STRING))
          .withClusteringColumn(KVColumn.primitive("ts2", "timestamp2", KVType.STRING))
          .build
      )
      .withFamily(
        ColumnFamilyBuilder
          .withName("otherCf")
          .withCellQualifier(KVColumn.primitive("otherField", "q3", KVType.BOOL))
          .build
      )
      .build
    val testCatalog = KeyValueModel.generateField(
      "testNs",
      "testTn",
      Some(kvSchema)
    )
    val res = HBaseTableCatalog(
      Map(
        HBaseTableCatalog.tableCatalog -> testCatalog,
        "avroSchema"                   -> avroSchema
      )
    )
    assert(
      res ===
        HBaseTableCatalog(
          "testNs",
          "testTn",
          RowKey("key"),
          SchemaMap(
            mutable.HashMap(
              "rowkey"      -> Field("rowkey", "rowkey", "key",Some("string")),
              "otherField"  -> Field("otherField", "otherCf", "q3", Some("boolean")),
              "field"       -> Field("field", "myCf", "qualifier", Some("long")),
              "timestamp"   -> Field("timestamp", "myCf", "timestamp", Some("string")),
              "structField" -> Field("structField", "myCf", "q2",None, Some(avroSchema)),
              "ts2"         -> Field("ts2", "myCf", "timestamp2",Some("string")),
              "ts"          -> Field("ts", "myCf", "timestamp",Some("string")),
              "timestamp2"  -> Field("timestamp2", "myCf", "timestamp2",Some("string"))
            )
          ),
          Map("myCf" -> Seq("timestamp", "timestamp2")),
          Map(
            "catalog"  -> testCatalog,
            "avroSchema" -> avroSchema
          )
        )
    )
  }
}
