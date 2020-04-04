package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.{KeyValueModel, KeyValueOption}

object TestKeyValueModel {

  lazy val hbase = KeyValueModel(
    name = "test_hbase",
    tableCatalog = KeyValueModel.generateField(
      namespace = "whitelabel",
      tableName = "test_table",
      Some(dfFieldsSchema)
    ),
    dataFrameSchema = None,
    options = Some(Seq(
      //KeyValueOption("hbase.spark.config.location", "/etc/hbase/conf/hbase-site.xml"), //org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf.HBASE_CONFIG_LOCATION
      KeyValueOption("newtable", "5"), //org.apache.spark.sql.datasources.hbase.HBaseTableCatalog.newTable
      KeyValueOption("nestedSchema", nestedAvroSchema)
    )),
    useAvroSchemaManager = false,
    avroSchemas = None
  )

  lazy val hbaseMultipleClusteringKeyValueModel = KeyValueModel(
    name = "test_hbase_multiple_clustering",
    tableCatalog = KeyValueModel.generateField(
      namespace = "whitelabel",
      tableName = "test_table_multi_clustering",
      Some(dfFieldsSchemaMultiClustering)
    ),
    dataFrameSchema = None,
    options = Some(Seq(
      //KeyValueOption("hbase.spark.config.location", "/etc/hbase/conf/hbase-site.xml"), //org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf.HBASE_CONFIG_LOCATION
      KeyValueOption("newtable", "5"), //org.apache.spark.sql.datasources.hbase.HBaseTableCatalog.newTable
      KeyValueOption("nestedSchema", nestedAvroSchema)
    )),
    useAvroSchemaManager = false,
    avroSchemas = None
  )

  private lazy val dfFieldsSchema =
    """
       |"id":{"cf":"rowkey", "col":"key", "type":"string"},
       |"number":{"cf":"c", "col":"my_number", "type":"int"},
       |"nested":{"cf":"c", "col":"my_nested", "avro":"nestedSchema"}
    """.stripMargin

  private lazy val dfFieldsSchemaMultiClustering =
    """
      |"id":{"cf":"rowkey", "col":"key", "type":"string"},
      |"number":{"cf":"c", "col":"my_number", "type":"int"},
      |"nested":{"cf":"c", "col":"my_nested", "avro":"nestedSchema"},
      |"number_clustering":{"cf":"n", "col":"my_number_clustering", "type":"int"},
      |"clustering_1":{"cf":"c", "columns":"id"},
      |"clustering_2":{"cf":"n", "columns":"id"}
    """.stripMargin

  private lazy val nestedAvroSchema =
    """
      | {
      |   "namespace": "it.agilelab.wasp.avro",
      |   "type": "record",
      |   "name": "metadata",
      |   "fields": [
      |       {"name": "field1", "type": "string"},
      |       {"name": "field2", "type": "long"},
      |       {"name": "field3", "type": "string"}
      |      ]
      | }
    """.stripMargin
}