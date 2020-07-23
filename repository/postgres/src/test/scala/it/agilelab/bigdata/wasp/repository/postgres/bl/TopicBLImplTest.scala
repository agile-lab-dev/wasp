package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.{MultiTopicModel, TopicCompression, TopicModel}
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite
import org.mongodb.scala.bson.BsonDocument

class TopicBLImplTest extends PostgresSuite{

  private val bl = TopicBLImpl(pgDB)

  it should "test topic bl" in {
    bl.createTable()

    val model1= TopicModel("name1",0L,10,3,"type",None,Some("header"),None,true,new BsonDocument(),TopicCompression.Lz4)
    val model2= MultiTopicModel("nameMulti","field",Seq("name1","name2"))

    bl.persist(model1)
    bl.persist(model2)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName(model2.name).get shouldBe model2
    bl.getByName("XXXXXXXX").isEmpty shouldBe true

    bl.getAll should contain theSameElementsAs Seq(model1,model2)

  }

}
