package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.FreeCodeModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite


trait FreeCodeBLImplTest {
  self : PostgresSuite =>

  private lazy val freeCodeBL = FreeCodeBLImpl(pgDB)


  it should "test freeCodeBL" in {

    freeCodeBL.createTable()

    val model1 = FreeCodeModel("test_1","code_1")
    freeCodeBL.insert(model1)

    val model2 = FreeCodeModel("test_2","code_2")
    freeCodeBL.insert(model2)

    val list = freeCodeBL.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1,model2)

    freeCodeBL.getByName(model1.name).get shouldBe model1
    freeCodeBL.getByName(model2.name).get shouldBe model2
    freeCodeBL.getByName("XXXX").isEmpty shouldBe true

    freeCodeBL.deleteByName(model1.name)
    freeCodeBL.deleteByName(model2.name)
    freeCodeBL.getAll.size shouldBe 0

  }

}
