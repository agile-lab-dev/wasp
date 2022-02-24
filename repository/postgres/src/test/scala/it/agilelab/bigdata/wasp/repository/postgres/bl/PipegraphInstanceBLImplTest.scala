package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

trait PipegraphInstanceBLImplTest {
  self : PostgresSuite =>


  private val bl =  PipegraphInstanceBlImpl(pgDB)


  it should "test PipegraphInstanceBlImpl" in {



    bl.dropTable()
    bl.createTable()

    val model1 = PipegraphInstanceModel("test_1","instance_1",100L,10L,PipegraphStatus.PENDING, None,None)
    bl.insert(model1)

    val model2 = PipegraphInstanceModel("test_2","instance_1",100L,10L,PipegraphStatus.PENDING, None,None)
    bl.insert(model2)

    val model3 = PipegraphInstanceModel("test_3","instance_2",100L,10L,PipegraphStatus.PENDING,None,None)
    bl.insert(model3)

    val list = bl.all()

    list.size shouldBe 3
    list should contain theSameElementsAs Seq(model1,model2,model3)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName(model2.name).get shouldBe model2
    bl.getByName(model3.name).get shouldBe model3
    bl.getByName("XXXX").isEmpty shouldBe true


    val instance1 = bl.instancesOf("instance_1")
    instance1.size shouldBe 2
    instance1 should contain theSameElementsAs Seq(model1,model2)


    val instance2 = bl.instancesOf("instance_2")
    instance2.size shouldBe 1
    instance2 should contain theSameElementsAs Seq(model3)

    val model4 = model3.copy(instanceOf = "instance_3")
    bl.getByName(model3.name).get should not be model4
    bl.update(model4) shouldBe model4

    bl.getByName(model3.name).get shouldBe model4

    bl.dropTable()

  }

}
