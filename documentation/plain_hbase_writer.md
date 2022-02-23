# Plain Hbase Writer

This Hbase plugin aims to be simple to use and configure.
It allows put and delete operations and accept dataframes with the following mandatory fields:

- operation: insert, update, delete-row or delete-cell
- rowKey
- columnFamily

Optional fields:
- value: in case of a delete-row/delete-cell value is null. 
Value is a map that contains values to be written to hbase, each key represent the column qualifier. 
Each key and value has to be of type `Array[Byte]`.

## Getting Started

Using this plugin is simply a matter of creating a witboost KeyValueModel, providing three options:
- hbase.spark.config.location: hbase-site.xml path
- HBaseTableCatalog.namespace: hbase table namespace
- HBaseTableCatalog.tableName: hbase table

`HBaseTableCatalog` contains other options

```
KeyValueModel(
      name = s"$etlName-$tableName-hbase-model",
      tableCatalog = "",
      dataFrameSchema = None,
      options = Some(Seq(KeyValueOption("hbase.spark.config.location", <location>),
            KeyValueOption(HBaseTableCatalog.namespace, <namespace>)),
            KeyValueOption(HBaseTableCatalog.tableName, <hbaseTable>)))
      useAvroSchemaManager = false,
      avroSchemas = None
    )
```

## Write dataframe schema

This plugin accept a dataframe with the following schema:

```
 |-- rowKey: binary (nullable = true)
 |-- operation: string (nullable = true)
 |-- columnFamily: binary (nullable = true)
 |-- values: map (nullable = true)
 |    |-- key: binary
 |    |-- value: binary (valueContainsNull = true)
```

Let's take for example a use case in which we have to insert incoming kafka serialized avro record to hbase.
This dataframe has the following columns: `key, value`.
In order to be validated by the plugin, we need to convert the dataframe to the accepted schema.
`HBaseWriterProperties` contains the dataframe field naming constants.

```
val df = spark 
.readStream 
.format("kafka") 
.option("kafka.bootstrap.servers", "host1:port1") 
.option("subscribe", "topic1") 
.load()

val toByteArray: UserDefinedFunction = udf((str: String) => {
    Bytes.toBytes(str)
  })

df
.withColumn(HBaseWriterProperties.RowkeyAttribute, "key")
.withColumn(HBaseWriterProperties.OperationAttribute, lit(HBaseWriterProperties.UpsertOperation))
.withColumn(HBaseWriterProperties.ColumnFamilyAttribute, lit(toByteArray("example-cf")))
.withColumn(HBaseWriterProperties.ValuesAttribute, 
                map(toByteArray(lit(toByteArray("qualifier")), col("value"))
           )
.select(col(HBaseWriterProperties.RowkeyAttribute),
        col(HBaseWriterProperties.OperationAttribute),
        col(HBaseWriterProperties.ColumnFamilyAttribute),
        col(HBaseWriterProperties.ValuesAttribute))
```

