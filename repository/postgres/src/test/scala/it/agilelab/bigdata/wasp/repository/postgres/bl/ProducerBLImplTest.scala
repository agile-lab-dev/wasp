package it.agilelab.bigdata.wasp.repository.postgres.bl

import java.sql.SQLException

import it.agilelab.bigdata.wasp.models.ProducerModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

class ProducerBLImplTest extends PostgresSuite{

  val bl = ProducerBLImpl(pgDB)

  it should "test producer bl" in {

    val p1 = ProducerModel("producer1","class",Some("topic1"),true,None,true,true)
    val p2 = ProducerModel("producer2","class",Some("topic2"),false,None,true,true)
    val p3 = ProducerModel("producer3","class",Some("topic2"),true,None,true,false)
    val p3Bis = ProducerModel("producer3","classBis",Some("topic2"),true,None,true,false)
    val p4 = ProducerModel("producer4","class",Some("topic1"),false,None,true,false)
    val p4Bis = ProducerModel("producer4","classBis",Some("topic1"),false,None,true,false)


    bl.createTable()


    bl.persist(p1)
    bl.insertIfNotExists(p2)
    bl.upsert(p3)
    bl.update(p4)
    bl.getByName(p4.name).isEmpty shouldBe true
    bl.persist(p4)

    bl.getByName(p1.name).get shouldBe p1
    bl.getByName(p2.name).get shouldBe p2
    bl.getByName(p3.name).get shouldBe p3
    bl.getByName(p4.name).get shouldBe p4

    bl.getActiveProducers(isActive = true) should contain theSameElementsAs List(p1,p3)
    bl.getActiveProducers(isActive = false) should contain theSameElementsAs List(p2,p4)

    bl.getSystemProducers should contain theSameElementsAs List(p1,p2)
    bl.getNonSystemProducers should contain theSameElementsAs List(p3,p4)

    bl.getByTopicName("topic1") should contain theSameElementsAs List(p1,p4)
    bl.getByTopicName("topic2") should contain theSameElementsAs List(p2,p3)


    an [SQLException] should be thrownBy bl.persist(p3Bis)
    bl.insertIfNotExists(p3Bis)
    bl.getByName(p3Bis.name).get should not be p3Bis
    bl.upsert(p3Bis)
    bl.getByName(p3Bis.name).get shouldBe p3Bis

    bl.insertIfNotExists(p4Bis)
    bl.getByName(p4Bis.name).get shouldBe p4
    bl.upsert(p4Bis)
    bl.getByName(p4Bis.name).get shouldBe p4Bis


  }

}
