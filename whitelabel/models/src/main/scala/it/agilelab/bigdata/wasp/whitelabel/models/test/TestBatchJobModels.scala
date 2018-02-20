package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

private[wasp] object TestBatchJobModels {

  object FromSolr {

    /**
      *  Fail if HDFS folder already exists
      */
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
        output = WriterModel.rawWriter("Raw Writer", TestRawModel.flatSchemaRawModel.name),
        mlModels = Nil,
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
  }

  object FromHdfs {

    /**
      *  Fail if HDFS folder does not exist
      */
    lazy val flatToConsole = BatchJobModel(
      name = "TestBatchJobFlatFromHdfsToConsole",
      description = "Description pf TestBatchJobFlatFromHdfsToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      state = JobStateEnum.PENDING,
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFlatFromHdfsToConsole",
        inputs = List(ReaderModel.rawReader("Raw Reader", TestRawModel.flatSchemaRawModel.name)),
        output = WriterModel.consoleWriter("Console"),
        mlModels = Nil,
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )

    /**
      *  Fail if HDFS folder does not exist
      */
    lazy val nestedToConsole = BatchJobModel(
      name = "TestBatchJobNestedFromHdfsToConsole",
      description = "Description pf TestBatchJobNestedFromHdfsToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      state = JobStateEnum.PENDING,
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobNestedFromHdfsToConsole",
        inputs = List(ReaderModel.rawReader("Raw Reader", TestRawModel.nestedSchemaRawModel.name)),
        output = WriterModel.consoleWriter("Console"),
        mlModels = Nil,
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
  }
}