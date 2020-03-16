package it.agilelab.bigdata.wasp.whitelabel.models.test

import java.net.InetAddress

import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.sql.types._

object TestGdprBatchJobModels {

  val hostname: String = InetAddress.getLocalHost.getCanonicalHostName

  lazy val dataRawModel: RawModel = RawModel(
    name = "GdprDataRawModel",
    uri = s"hdfs://$hostname:9000/user/root/gdpr/data",
    timed = false,
    schema = StructType(Seq(
      StructField("id", StringType),
      StructField("number", LongType),
      StructField("name", StringType)
    )).json)

  lazy val dataWithDateRawModel: RawModel = RawModel(
    name = "GdprDataWithDateRawModel",
    uri = s"hdfs://$hostname:9000/user/root/gdpr/datawithdate",
    timed = false,
    schema = StructType(Seq(
      StructField("id", StringType),
      StructField("category", StringType),
      StructField("date", StringType),
      StructField("name", StringType)
    )).json,
    RawOptions("append", "parquet", None, Some(List("category")))
  )

  lazy val dataKeyValueModel: KeyValueModel = KeyValueModel(
    name = "GdprDataRawModel",
    tableCatalog = KeyValueModel.generateField("dev", "data", None),
    dataFrameSchema = None,
    options = None,
    useAvroSchemaManager = false,
    avroSchemas = None
  )

  val dataStores: List[DataStoreConf] = List(
    RawDataStoreConf(
      "key",
      dataWithDateRawModel,
      PrefixRawMatchingStrategy("id"),
      TimeBasedBetweenPartitionPruningStrategy("date", isDateNumeric = false, "yyyyMMddHHmm")
    ),
    RawDataStoreConf(
      "key",
      dataRawModel,
      PrefixRawMatchingStrategy("id"),
      NoPartitionPruningStrategy()
    ),
    KeyValueDataStoreConf(
      "key",
      dataKeyValueModel,
      PrefixAndTimeBoundKeyValueMatchingStrategy("|", isDateFirst = false, "yyyyMMddHHmm", "IT")
    )
  )

  lazy val inputRawModel: RawModel = RawModel(
    name = "GdprInputRawModel",
    uri = s"hdfs://$hostname:9000/user/root/gdpr/input",
    timed = false,
    schema = StructType(Seq(StructField("key", StringType))).json)

  lazy val inputs = List(
    ReaderModel.rawReader(
      name = "GdprInputRawModel",
      rawModel = inputRawModel
    )
  )

  lazy val outputRawModel: RawModel = RawModel(
    name = "GdprOutputRawModel",
    uri = s"hdfs://$hostname:9000/user/root/gdpr/result/",
    timed = false,
    schema = StructType(Seq(
      StructField("key", StringType),
      StructField("result", BooleanType)
    )).json)

  lazy val output: WriterModel = WriterModel.rawWriter(
    name = outputRawModel.name,
    rawModel = outputRawModel
  )

  lazy val model: BatchJobModel = BatchJobModel(
    name = "gdprBatchJob",
    description = "gdpr deletion test model",
    owner = "user",
    system = false,
    creationTime = System.currentTimeMillis(),
    etl = BatchGdprETLModel(
      name = "gpdrETLModel",
      dataStores = dataStores,
      strategyConfig = "",
      inputs = inputs,
      output = output
    )
  )

}
