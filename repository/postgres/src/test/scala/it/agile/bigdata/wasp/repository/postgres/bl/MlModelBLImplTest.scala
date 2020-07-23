package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.utils.PostgresSuite
import it.agilelab.bigdata.wasp.models.{FreeCodeModel, MlModelOnlyInfo}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

class MlModelBLImplTest extends PostgresSuite {
  private val bl = MlModelBLImpl(pgDB)

  it should "test MlModelBLImpl for postgres" in {

    bl.createTable()


    val file1 = FreeCodeModel("name","code")
    val file2 =FreeCodeModel("name2","code2")


    val id1= bl.saveTransformer( file1,"name1","1",18)
    val id2= bl.saveTransformer(file2 ,"name1","1",18)



    val model1 = MlModelOnlyInfo("name","1",Some("class"),Some(3),Some(id1),true,"description")
    val model2 = MlModelOnlyInfo("name","1",Some("class"),Some(5),Some(id2),true,"description")
    val model3 = MlModelOnlyInfo("name","1",Some("class"),Some(7),Some(new BsonObjectId(new ObjectId("%024d".format(123)))),true,"description")

    bl.getSerializedTransformer(model1).get.asInstanceOf[FreeCodeModel] shouldBe file1
    bl.getSerializedTransformer(model2).get.asInstanceOf[FreeCodeModel] shouldBe file2
    bl.getSerializedTransformer(model3).isEmpty shouldBe true


    bl.saveMlModelOnlyInfo(model1)
    bl.saveMlModelOnlyInfo(model2)
    bl.saveMlModelOnlyInfo(model3)

    bl.getAll should contain theSameElementsAs Seq(model1,model2,model3)
    bl.getMlModelOnlyInfo(model1.name,model1.version,model1.timestamp.get).get shouldBe model1
    bl.getMlModelOnlyInfo(model2.name,model2.version,model2.timestamp.get).get shouldBe model2
    bl.getMlModelOnlyInfo(model3.name,model3.version,model3.timestamp.get).get shouldBe model3

    bl.getMlModelOnlyInfo(model1.name,model1.version).get shouldBe model3

    bl.delete(model3.name,model3.version,model3.timestamp.get)

    bl.getMlModelOnlyInfo(model1.name,model1.version).get shouldBe model2

    bl.delete(model2.name,model2.version,model2.timestamp.get)
    bl.delete(model1.name,model1.version,model1.timestamp.get)

    bl.getMlModelOnlyInfo(model1.name,model1.version).isEmpty shouldBe true
    bl.getSerializedTransformer(model1).isEmpty shouldBe true
    bl.getSerializedTransformer(model2).isEmpty shouldBe true



  }

}
