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
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromSolr",
        inputs = List(ReaderModel.solrReader("Solr Reader", TestIndexModel.solr.name)),
        output = WriterModel.rawWriter("Raw Writer", TestRawModel.flat.name),
        mlModels = List(),
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      ),
      state = JobStateEnum.PENDING
    )
  }

  object FromHdfs {

    /**
      *  Fail if HDFS folder does not exist
      */
    lazy val flatToConsole = BatchJobModel(
      name = "TestBatchJobFromHdfsFlatToConsole",
      description = "Description of TestBatchJobFromHdfsFlatToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromHdfsFlatToConsole",
        inputs = List(ReaderModel.rawReader("Raw Reader", TestRawModel.flat.name)),
        output = WriterModel.consoleWriter("Console"),
        mlModels = List(),
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      ),
      state = JobStateEnum.PENDING
    )

    /**
      *  Fail if HDFS folder does not exist
      */
    lazy val nestedToConsole = BatchJobModel(
      name = "TestBatchJobFromHdfsNestedToConsole",
      description = "Description of TestBatchJobFromHdfsNestedToConsole",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromHdfsNestedToConsole",
        inputs = List(ReaderModel.rawReader("Raw Reader", TestRawModel.nested.name)),
        output = WriterModel.consoleWriter("Console"),
        mlModels = List(),
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.IdentityStrategy")),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      ),
      state = JobStateEnum.PENDING
    )
  }
}