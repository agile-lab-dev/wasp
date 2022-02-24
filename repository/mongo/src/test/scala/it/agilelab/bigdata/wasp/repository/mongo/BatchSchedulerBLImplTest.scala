package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.BatchSchedulerModel
import it.agilelab.bigdata.wasp.repository.mongo.bl.BatchSchedulersBLImp
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class BatchSchedulerBLImplTest extends FlatSpec with Matchers {


  it should "test BatchSchedulerBLImpl on Mongo " in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new BatchSchedulersBLImp(waspDB)

    val model1 = BatchSchedulerModel("name1", "string", None, None, true)
    val model2 = BatchSchedulerModel("name2", "string", None, None, true)
    val model3 = BatchSchedulerModel("name3", "string", None, None, false)
    val model4 = BatchSchedulerModel("name4", "string", None, None, false)

    bl.persist(model1)
    bl.persist(model2)
    bl.persist(model3)
    bl.persist(model4)

    val active = bl.getActiveSchedulers(true)
    val inactive = bl.getActiveSchedulers(false)

    active should contain theSameElementsAs Seq(model1,model2)
    inactive should contain theSameElementsAs Seq(model3,model4)
  }


}
