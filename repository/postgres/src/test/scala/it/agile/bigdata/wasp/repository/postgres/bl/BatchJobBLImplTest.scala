package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.utils.PostgresSuite
import it.agilelab.bigdata.wasp.models.{BatchETLModel, BatchJobModel, WriterModel}

class BatchJobBLImplTest extends PostgresSuite{

  val batchJobBL =  BatchJobBLImpl(pgDB)


  it should "test batchJobBL" in {

    batchJobBL.createTable()

    val etl = BatchETLModel("name",List.empty,WriterModel.consoleWriter("test"),List.empty,None,"kafka")
    val model1 = BatchJobModel("name","description","tester",true,10L,etl)
    batchJobBL.insert(model1)

    val model2 = BatchJobModel("name2","description2","tester",true,10L,etl)
    batchJobBL.insert(model2)

    val list = batchJobBL.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1,model2)

    batchJobBL.getByName(model1.name).get shouldBe model1
    batchJobBL.getByName(model2.name).get shouldBe model2
    batchJobBL.getByName("XXXX").isEmpty shouldBe true

    batchJobBL.deleteByName(model1.name)
    batchJobBL.deleteByName(model2.name)
    batchJobBL.getAll.size shouldBe 0

  }


  it should "test batchJobBL update" in {

    batchJobBL.createTable()

    val etl = BatchETLModel("name",List.empty,WriterModel.consoleWriter("test"),List.empty,None,"kafka")
    val model1 = BatchJobModel("nameUpdate","description","tester",true,10L,etl)
    batchJobBL.insert(model1)

    batchJobBL.getByName(model1.name).get shouldBe model1

    val model2 = BatchJobModel("nameUpdate","description2","tester",true,10L,etl)
    batchJobBL.update(model2)

    batchJobBL.getByName(model1.name).get shouldBe model2

    batchJobBL.deleteByName(model1.name)



  }

  it should "test batchJobBL upsert" in {

    batchJobBL.createTable()

    val etl = BatchETLModel("name",List.empty,WriterModel.consoleWriter("test"),List.empty,None,"kafka")
    val model1 = BatchJobModel("nameUpsert","description","tester",true,10L,etl)
    batchJobBL.upsert(model1)

    batchJobBL.getByName(model1.name).get shouldBe model1

    val model2 = BatchJobModel("nameUpsert","description2","tester",true,10L,etl)
    batchJobBL.upsert(model2)

    batchJobBL.getByName(model1.name).get shouldBe model2

    batchJobBL.deleteByName(model1.name)



  }


}
