package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.{FreeCodeModel, MlModelOnlyInfo}
import it.agilelab.bigdata.wasp.repository.mongo.bl.MlModelBLImp
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class MlModelBLImplTest extends FlatSpec with Matchers {

  it should "test MlModelBLImpl for mongo" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new MlModelBLImp(waspDB)

    val file1 = FreeCodeModel("name", "code")
    val file2 = FreeCodeModel("name2", "code2")


    val id1 = bl.saveTransformer(file1, "name1", "mlModelV1", 18)
    val id2 = bl.saveTransformer(file2, "name1", "mlModelV1", 18)
    val id3 = bl.saveTransformer(file2, "name3", "mlModelV1", 18)


    val model1 = MlModelOnlyInfo("name","mlModelV1" , Some("class"), Some(3), Some(id1), true, "description")
    val model2 = MlModelOnlyInfo("name","mlModelV1" , Some("class"), Some(5), Some(id2), true, "description")
    val model3 = MlModelOnlyInfo("name","mlModelV1" , Some("class"), Some(7), Some(id3), true, "description")

    bl.saveMlModelOnlyInfo(model1)
    bl.saveMlModelOnlyInfo(model2)
    bl.saveMlModelOnlyInfo(model3)

    bl.getAll should contain theSameElementsAs Seq(model1, model2, model3)
    bl.getMlModelOnlyInfo(model1.name, model1.version, model1.timestamp.get).get shouldBe model1
    bl.getMlModelOnlyInfo(model2.name, model2.version, model2.timestamp.get).get shouldBe model2
    bl.getMlModelOnlyInfo(model3.name, model3.version, model3.timestamp.get).get shouldBe model3

    bl.getMlModelOnlyInfo(model1.name, model1.version).get shouldBe model3

    bl.delete(model3.name, model3.version, model3.timestamp.get)

    bl.getMlModelOnlyInfo(model2.name, model2.version).get shouldBe model2

    bl.delete(model2.name, model2.version, model2.timestamp.get)
    bl.delete(model1.name, model1.version, model1.timestamp.get)

    bl.getMlModelOnlyInfo(model1.name, model1.version).isEmpty shouldBe true
//    bl.getSerializedTransformer(model1).isEmpty shouldBe true
//    bl.getSerializedTransformer(model2).isEmpty shouldBe true
  }
}
