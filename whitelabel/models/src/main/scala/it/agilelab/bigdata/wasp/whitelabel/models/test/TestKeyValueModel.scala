package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.{KeyValueModel, KeyValueOption}

object TestKeyValueModel {
  val tableName = "test_hbase"
  val namespace = "mattia_bertorello"

  val nestedAvroSchema = s"""   {"namespace": "it.agilelab.wasp.avro",
                        		  |   "type": "record", "name": "metadata",
                        		  |    "fields": [
                        		  |        {"name": "field1", "type": "string"},
                        			|        {"name": "field2", "type": "long"},
                        			|        {"name": "field3", "type": "string"}
                        			|      ]
                        		  |  }""".stripMargin

  val dfFieldsSchema: String =
    s"""
       |"id":{"cf":"rowkey", "col":"key", "type":"string"},
       |"number":{"cf":"c", "col":"crash", "type":"int"},
       |"nested":{"cf":"c", "col":"nested", "avro":"nestedAvroSchema"}
       |""".stripMargin


  val hbaseSchema: String = KeyValueModel.generateField(namespace, tableName, Some(dfFieldsSchema))

  /* for Pipegraph */
  lazy val simple = KeyValueModel(
    tableName,
    hbaseSchema,
    None,
    Some(Seq(
      //KeyValueOption("hbase.spark.config.location", "/etc/hbase/conf/hbase-site.xml"), //org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf.HBASE_CONFIG_LOCATION
      KeyValueOption("newtable", "5"), //org.apache.spark.sql.datasources.hbase.HBaseTableCatalog.newTable
      KeyValueOption("nestedAvroSchema", nestedAvroSchema) //org.apache.spark.sql.datasources.hbase.HBaseTableCatalog.newTable
    )),
    None
  )
}
