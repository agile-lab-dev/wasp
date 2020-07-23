package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

class BatchSchedulersBLImplTest extends PostgresSuite {

  private val bl = BatchSchedulersBLImpl(pgDB)

  it should "Test BatchSchedulersBLImpl" in {

    val model1 = BatchSchedulerModel("name1","string",None,None,true)
    val model2 = BatchSchedulerModel("name2","string",None,None,true)
    val model3 = BatchSchedulerModel("name3","string",None,None,false)
    val model4 = BatchSchedulerModel("name4","string",None,None,false)


    bl.createTable()
    bl.persist(model1)
    bl.persist(model2)
    bl.persist(model3)
    bl.persist(model4)

    bl.getActiveSchedulers(true) should contain theSameElementsAs Seq(model1,model2)
    bl.getActiveSchedulers(false) should contain theSameElementsAs Seq(model3,model4)


  }

}
