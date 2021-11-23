package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.mongo.bl.BatchJobBLImp
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class BatchJobBLImplTest extends FlatSpec with Matchers {

  it should "test batchJobBL" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB     = db.getDB()
    val batchJobBL = new BatchJobBLImp(waspDB)

    val etl  = BatchETLModel("name", List.empty, WriterModel.consoleWriter("test"), List.empty, None, "kafka")
    val gdpr = BatchGdprETLModel("gdpr", List.empty, "string", List.empty, WriterModel.consoleWriter("test"), "lafka")

    val model2 = BatchJobModel("name2", "description2", "tester", true, 10L, gdpr)
    batchJobBL.insert(model2)

    val model1 = BatchJobModel("name", "description", "tester", true, 12L, etl)
    batchJobBL.insert(model1)

    val list = batchJobBL.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1, model2)

    batchJobBL.getByName(model1.name).get shouldBe model1
    batchJobBL.getByName(model2.name).get shouldBe model2
    batchJobBL.getByName("XXXX").isEmpty shouldBe true

    batchJobBL.deleteByName(model1.name)
    batchJobBL.deleteByName(model2.name)
    batchJobBL.getAll.size shouldBe 0

  }

  it should "test batchJobBL update" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB     = db.getDB()
    val batchJobBL = new BatchJobBLImp(waspDB)

    val etl    = BatchETLModel("name", List.empty, WriterModel.consoleWriter("test"), List.empty, None, "kafka")
    val model1 = BatchJobModel("nameUpdate", "description", "tester", true, 10L, etl)
    batchJobBL.insert(model1)

    batchJobBL.getByName(model1.name).get shouldBe model1

    val model2 = BatchJobModel("nameUpdate", "description2", "tester", true, 10L, etl)
    batchJobBL.update(model2)

    batchJobBL.getByName(model1.name).get shouldBe model2

    batchJobBL.deleteByName(model1.name)

  }

  it should "test batchJobBL upsert" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB     = db.getDB()
    val batchJobBL = new BatchJobBLImp(waspDB)

    val etl    = BatchETLModel("name", List.empty, WriterModel.consoleWriter("test"), List.empty, None, "kafka")
    val model1 = BatchJobModel("nameUpsert", "description", "tester", true, 10L, etl)
    batchJobBL.upsert(model1)

    batchJobBL.getByName(model1.name).get shouldBe model1

    val model2 = BatchJobModel("nameUpsert", "description2", "tester", true, 10L, etl)
    batchJobBL.upsert(model2)

    batchJobBL.getByName(model1.name).get shouldBe model2

    batchJobBL.deleteByName(model1.name)

  }

  it should "test batchJobBL upsert of gdpr deletion job" in {

    val gdprDeletionInput: RawModel =
      RawModel(
        name = "GDPRKeysToDelete",
        uri = "hdfs://abc/toBeDeleted=true",
        timed = false,
        schema = "",
        new RawOptions(
          "append",
          "parquet",
          None,
          Some(List("toBeDeleted", "RUN_ID"))
        )
      )

    val gdprDeletionResult: RawModel = RawModel(
      "GDPRDeletionResult",
      uri = "hdfs://gdpr/deletionResultUri",
      timed = false,
      schema = "{}",
      new RawOptions(
        "append",
        "parquet",
        None,
        Some(List("RUN_ID"))
      )
    )

    val dataStoresToDelete: List[DataStoreConf] = {
      val exactMatching = RawDataStoreConf(
        "abc",
        "dbc",
        RawModel(
          "nome",
          "hdfs://raw/model/uri",
          false,
          "{}",
          RawOptions("append", "parquet", Some(Map("k" -> "v")), Some(List("k")))
        ),
        ExactRawMatchingStrategy("abc"),
        NoPartitionPruningStrategy()
      )
      val containsMatching  = exactMatching.copy(rawMatchingStrategy = ContainsRawMatchingStrategy("abc"))
      val prefixRawMatching = exactMatching.copy(rawMatchingStrategy = PrefixRawMatchingStrategy("abc"))
      val partitionPruning = exactMatching.copy(partitionPruningStrategy =
        TimeBasedBetweenPartitionPruningStrategy("t", isDateNumeric = false, "yyyyMMdd", "hours")
      )

      val kvExact = KeyValueDataStoreConf(
        inputKeyColumn = "input",
        correlationIdColumn = "corrId",
        keyValueModel = KeyValueModel(
          "name",
          "{}",
          None,
          Some(Seq(KeyValueOption("k", "v"))),
          false,
          Some(Map("a" -> "{}"))
        ),
        keyValueMatchingStrategy = ExactKeyValueMatchingStrategy()
      )
      val kvPrefixAndTime =
        kvExact.copy(keyValueMatchingStrategy = PrefixAndTimeBoundKeyValueMatchingStrategy("|", "yyyyMMdd"))
      val kvPrefix = kvExact.copy(keyValueMatchingStrategy = PrefixKeyValueMatchingStrategy())

      List(
        exactMatching,
        containsMatching,
        prefixRawMatching,
        partitionPruning,
        kvExact,
        kvPrefix,
        kvPrefixAndTime
      )
    }

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB     = db.getDB()
    val batchJobBL = new BatchJobBLImp(waspDB)

    val gdprDeletionInputModel = gdprDeletionInput
    val batchJob = BatchJobModel(
      name = "GDPRDeletion",
      description = "",
      owner = "user",
      system = false,
      creationTime = System.currentTimeMillis(),
      etl = BatchGdprETLModel(
        name = "GDPR Deletion ETL",
        inputs = List(
          ReaderModel.rawReader(
            gdprDeletionInputModel.name,
            gdprDeletionInputModel
          )
        ),
        output = WriterModel.rawWriter(
          gdprDeletionResult.name,
          gdprDeletionResult
        ),
        strategyConfig = "",
        dataStores = dataStoresToDelete
      ),
      exclusivityConfig = BatchJobExclusionConfig(isFullyExclusive = true, Seq.empty)
    )

    batchJobBL.upsert(batchJob)

    batchJobBL.getByName(batchJob.name).get shouldBe batchJob

    batchJobBL.upsert(batchJob.copy(owner = "vez"))

    batchJobBL.getByName(batchJob.name).get shouldBe batchJob.copy(owner = "vez")

    batchJobBL.deleteByName(batchJob.name)

  }

}
