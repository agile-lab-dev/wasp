package it.agilelab.bigdata.wasp.repository.mongo

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, JobStatus}
import it.agilelab.bigdata.wasp.repository.mongo.bl.BatchJobInstanceBlImp
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

import scala.util.Random

@DoNotDiscover
class BatchJobInstanceBLImplTest extends FlatSpec with Matchers{

  it should "test batchJobInstanceBL" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new BatchJobInstanceBlImp(waspDB)

    println("==========================")
    val list2 = bl.all()
    for(i <- list2)
      println(i)
    println("==========================")

    // add random names for keys
    def key: Int = Random.nextInt(10000)

    val model1 = BatchJobInstanceModel(s"name_$key", "instance_1",100L,10L,JobStatus.SUCCESSFUL)
    bl.insert(model1)

    val model2 = BatchJobInstanceModel(s"name_$key", "instance_1",100L,10L,JobStatus.SUCCESSFUL,ConfigFactory.empty(),Some("error"))
    bl.insert(model2)

    val model3 = BatchJobInstanceModel(s"name_$key", "instance_2",100L,10L,JobStatus.SUCCESSFUL)
    bl.insert(model3)

    val list = bl.all()
    for(i <- list)
      println(i)

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

  }
}
