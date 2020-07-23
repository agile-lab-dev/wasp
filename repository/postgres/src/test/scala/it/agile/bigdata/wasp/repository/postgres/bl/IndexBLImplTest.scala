package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.utils.PostgresSuite
import it.agilelab.bigdata.wasp.models.IndexModel

class IndexBLImplTest extends PostgresSuite{

  private val bl = IndexBLImpl(pgDB)

  it should "test IndexBLImpl" in {

    bl.createTable()

    val model1 = IndexModel("name_1",10L,None,None,Some(3),None,true,Some("test"))
    bl.persist(model1)

    val model2 = IndexModel("name_2",10L,None,None,Some(3),None,true,Some("test"))
    bl.persist(model2)

    val list = bl.getAll()

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1,model2)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName(model2.name).get shouldBe model2
    bl.getByName("XXXX").isEmpty shouldBe true


  }

  it should "test upsert/insert" in {

    bl.createTable()

    val model1 = IndexModel("test_1",10L,None,None,Some(3),None,true,Some("test"))
    val model1Bis = IndexModel("test_1",100L,None,None,Some(3),None,true,Some("test"))

    val model2 = IndexModel("test_2",10L,None,None,Some(3),None,true,Some("test"))
    val model3 = IndexModel("test_3",10L,None,None,Some(3),None,true,Some("test"))

    bl.persist(model1)
    bl.getByName(model1.name).get shouldBe model1
    model1Bis should not be model1
    bl.insertIfNotExists(model1Bis)
    bl.getByName(model1.name).get shouldBe model1
    bl.upsert(model1Bis)
    bl.getByName(model1.name).get shouldBe model1Bis

    bl.insertIfNotExists(model2)
    bl.getByName(model2.name).get shouldBe model2

    bl.upsert(model3)
    bl.getByName(model3.name).get shouldBe model3


  }

}
