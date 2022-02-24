package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.mongo.bl.ProcessGroupBLImpl
import org.mongodb.scala.bson.{BsonDocument, BsonString}
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class ProcessGroupBLImplTest extends FlatSpec with Matchers{

  it should "test processGroupBL for Mongo" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new ProcessGroupBLImpl(waspDB)

    val bson   = BsonDocument(("testVal", "ciao"))
    val model1 = ProcessGroupModel("name", bson, "errport")
    bl.insert(model1)

    val bson2   = BsonDocument(("testVal", "ciao2"))
    val model2 = ProcessGroupModel("name2", bson2, "errport")
    bl.insert(model2)

    bl.getById(model1.name).get shouldBe model1
    bl.getById(model2.name).get shouldBe model2
    bl.getById("XXXX").isEmpty shouldBe true

    bl.getById(model1.name).get.content.get("testVal") shouldBe BsonString("ciao")
    bl.getById(model2.name).get.content.get("testVal") shouldBe BsonString("ciao2")

  }

}