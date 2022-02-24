package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.{MultiTopicModel, TopicCompression, TopicModel}
import it.agilelab.bigdata.wasp.repository.mongo.bl.TopicBLImp
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}


@DoNotDiscover
class TopicBLImplTest extends FlatSpec with Matchers{

  it should "test TopicBLImpl for Mongo" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new TopicBLImp(waspDB)

    val model1= TopicModel("name1",0L,10,3,"type",Option("keyField"), None,Option(Seq("thisOption","thatOption")),true,new BsonDocument(),TopicCompression.Lz4)
    val model2= MultiTopicModel("nameMulti","field",Seq("name1","name2"))

    bl.persist(model1)
    bl.persist(model2)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName(model2.name).get shouldBe model2
    bl.getByName("XXXXXXXX").isEmpty shouldBe true

    bl.getAll should contain theSameElementsAs Seq(model1,model2)

  }
}

