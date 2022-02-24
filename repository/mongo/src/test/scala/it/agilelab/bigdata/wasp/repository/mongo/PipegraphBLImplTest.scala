package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.mongo.bl.{PipegraphBLImp, PipegraphInstanceBlImp}
import org.bson.BsonDocument
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class PipegraphBLImplTest extends FlatSpec with Matchers{

  it should "test PipegraphBLImpl" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new PipegraphBLImp(waspDB)

    val model1 = PipegraphModel("name1", "description", "tester", true, 10L, List.empty, None)
    bl.insert(model1)

    val etl = StructuredStreamingETLModel("name_1", "not-default",
      StreamingReaderModel.topicReader("name", TopicModel("name", 10L, 3, 3, "topic", None, None, None, true, new BsonDocument), Some(10), Map.empty),
      List.empty,
      WriterModel.consoleWriter("console"),
      List.empty, None, Some(10))
    val model2 = PipegraphModel("name2", "description", "tester", true, 10L, List(etl), None)
    bl.insert(model2)

    val list = bl.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1, model2)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName(model2.name).get shouldBe model2
    bl.getByName("XXXX").isEmpty shouldBe true

    bl.deleteByName(model1.name)
    bl.deleteByName(model2.name)
    bl.getAll.size shouldBe 0

  }

  it should "test insert/upsert/update" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new PipegraphBLImp(waspDB)


    val model1 = PipegraphModel("name_1", "description1", "tester", true, 10L, List.empty, None)
    val model2 = PipegraphModel("name_1", "description2", "tester", true, 10L, List.empty, None)
    val model3 = PipegraphModel("name_2", "description3", "tester", true, 10L, List.empty, None)
    val model4 = PipegraphModel("name_2", "description4", "tester", true, 10L, List.empty, None)


    bl.insert(model1)
    bl.getByName(model1.name).get shouldBe model1

//    an[Exception] should be thrownBy bl.insert(model2)  no exception because mongoDB objects are not unique on name, but on ObjectId
    bl.upsert(model2)
    bl.getByName(model1.name).get shouldBe model2


    bl.update(model3)
    bl.getByName(model3.name).isEmpty shouldBe true
    bl.upsert(model3)
    bl.getByName(model3.name).get shouldBe model3
    bl.update(model4)
    bl.getByName(model3.name).get shouldBe model4


    bl.deleteByName(model1.name)
    bl.deleteByName(model2.name)
    bl.deleteByName(model3.name)
    bl.deleteByName(model4.name)
    bl.getAll.size shouldBe 0
  }


  it should "test getSystemPipegraphs" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new PipegraphBLImp(waspDB)

    val model1 = PipegraphModel("model_1", "description1", "tester", true, 10L, List.empty, None)
    val model2 = PipegraphModel("model_2", "description2", "tester", true, 10L, List.empty, None)
    val model3 = PipegraphModel("model_3", "description3", "tester", false, 10L, List.empty, None)
    val model4 = PipegraphModel("model_4", "description4", "tester", false, 10L, List.empty, None)


    bl.insert(model1)
    bl.insert(model2)
    bl.insert(model3)
    bl.insert(model4)

    val all = bl.getAll
    all.size shouldBe 4
    all should contain theSameElementsAs Seq(model1, model2, model3, model4)

    val systemPipegraphs = bl.getSystemPipegraphs
    systemPipegraphs.size shouldBe 2
    systemPipegraphs should contain theSameElementsAs Seq(model1, model2)

    val notSystemPipegraphs = bl.getNonSystemPipegraphs
    notSystemPipegraphs.size shouldBe 2
    notSystemPipegraphs should contain theSameElementsAs Seq(model3, model4)


    bl.deleteByName(model1.name)
    bl.deleteByName(model2.name)
    bl.deleteByName(model3.name)
    bl.deleteByName(model4.name)
    bl.getAll.size shouldBe 0


  }

  it should "test getActivePipegraphs" in {


    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new PipegraphBLImp(waspDB)
    val blInstance = new PipegraphInstanceBlImp(waspDB)

    val modelInstance1 = PipegraphInstanceModel("name_i_1", "name_1", 100L, 10L, PipegraphStatus.PENDING, None, None)
    val modelInstance2 = PipegraphInstanceModel("name_i_2", "name_2", 100L, 10L, PipegraphStatus.PROCESSING, None, None)
    val modelInstance3 = PipegraphInstanceModel("name_i_3", "name_3", 100L, 10L, PipegraphStatus.FAILED, None, None)
    val modelInstance4 = PipegraphInstanceModel("name_i_4", "name_4", 100L, 10L, PipegraphStatus.PROCESSING, None, None)


    blInstance.insert(modelInstance1)
    blInstance.insert(modelInstance2)
    blInstance.insert(modelInstance3)
    blInstance.insert(modelInstance4)

    blInstance.all() should contain theSameElementsAs Seq(modelInstance1, modelInstance2, modelInstance3, modelInstance4)


    val model1 = PipegraphModel("name_1", "description1", "tester", true, 10L, List.empty, None)
    val model2 = PipegraphModel("name_2", "description2", "tester", true, 10L, List.empty, None)
    val model3 = PipegraphModel("name_3", "description3", "tester", false, 10L, List.empty, None)
    bl.insert(model1)
    bl.insert(model2)
    bl.insert(model3)
    bl.getAll should contain theSameElementsAs Seq(model1, model2, model3)


    bl.getActivePipegraphs() should contain theSameElementsAs Seq(model1, model2)

    bl.deleteByName(model1.name)
    bl.deleteByName(model2.name)
    bl.deleteByName(model3.name)
    bl.getAll.size shouldBe 0

  }
}
