package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite
import org.mongodb.scala.bson.{BsonDocument, BsonString}

trait ProcessGroupBLImplTest {
  self : PostgresSuite =>


  private lazy val processGroupBL = ProcessGroupBLImpl(pgDB)
  
  it should "test processGroupBL" in {

    processGroupBL.createTable()

    val bson   = BsonDocument(("testVal", "ciao"))
    val model1 = ProcessGroupModel("name", bson, "errport")
    processGroupBL.insert(model1)

    val bson2   = BsonDocument(("testVal", "ciao2"))
    val model2 = ProcessGroupModel("name2", bson2, "errport")
    processGroupBL.insert(model2)

    processGroupBL.getById(model1.name).get shouldBe model1
    processGroupBL.getById(model2.name).get shouldBe model2
    processGroupBL.getById("XXXX").isEmpty shouldBe true

    processGroupBL.getById(model1.name).get.content.get("testVal") shouldBe BsonString("ciao")
    processGroupBL.getById(model2.name).get.content.get("testVal") shouldBe BsonString("ciao2")

  }

}
