package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

private[wasp] object TestBatchJobModels {

  object FromSolr {

    lazy val toHdfs = BatchJobModel(
      name = "TestBatchJobFromSolrToHdfs",
      description = "Description pf TestBatchJobFromSolr",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      state = JobStateEnum.PENDING,
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromSolr",
        inputs = List(ReaderModel.solrReader("Solr Reader", TestIndexModel().name)),
        output = WriterModel.rawWriter("Raw Writer", TestRawModel().name),
        mlModels = Nil,
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
  }

  object FromHdfs {

    lazy val toConsole = BatchJobModel(
      name = "TestBatchJobFromHdfsToConsole",
      description = "Description pf TestBatchJobFromHdfsToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      state = JobStateEnum.PENDING,
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromHdfsToConsole",
        inputs = List(ReaderModel.rawReader("Raw Reader", TestRawModel().name)),
        output = WriterModel.consoleWriter("Console"),
        mlModels = Nil,
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
  }

  def main(args: Array[String]): Unit = {
    val nested = StructType(Seq(StructField("pippo", IntegerType)))

    val dt = StructType(Seq(StructField("name", LongType), StructField("nested", nested)))

    println(dt.prettyJson)
  }
}
