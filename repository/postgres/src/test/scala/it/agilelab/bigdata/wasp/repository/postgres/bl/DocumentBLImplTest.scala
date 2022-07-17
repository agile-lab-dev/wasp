package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

trait DocumentBLImplTest {
  self: PostgresSuite =>

  private lazy val documentBL = DocumentBLImpl(pgDB)

  it should "test documentBL" in {

    documentBL.createTable()

    val model1 = DocumentModel("name", "conn", "schema")
    val model2 = DocumentModel("name2", "conn2", "schema2")

    documentBL.persist(model1)

    documentBL.persist(model2)

    val list = documentBL.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1, model2)

    documentBL.getByName(model1.name).get shouldBe model1
    documentBL.getByName(model2.name).get shouldBe model2
    documentBL.getByName("XXXX").isEmpty shouldBe true

    documentBL.getAll.size shouldBe 2

  }

  it should "test documentBL upsert" in {

    documentBL.createTable()

    val model3 = DocumentModel("name3", "conn", "schema")
    val model4 = DocumentModel("name3", "conn2", "schema2")

    documentBL.upsert(model3)

    documentBL.getByName(model3.name).get shouldBe model3
    
    documentBL.upsert(model4)

    documentBL.getByName(model3.name).get shouldBe model4

    documentBL.getAll.filter(model => model.name == model3.name).head shouldBe model4

  }
}
