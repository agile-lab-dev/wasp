package it.agilelab.bigdata.wasp.repository.mongo

import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.mongo.bl.{BatchJobBLImp, BatchJobInstanceBlImp}
import org.scalatest.{BeforeAndAfter, DoNotDiscover, FlatSpec, FunSuite, Matchers}

import java.util.UUID

@DoNotDiscover
class BatchJobBLImplTest extends FlatSpec with Matchers{

    it should "test batchJobBL" in {

      val db = WaspMongoDB
      db.initializeDB()
      val waspDB = db.getDB()
      val batchJobBL = new BatchJobBLImp(waspDB)

      val etl = BatchETLModel("name",List.empty,WriterModel.consoleWriter("test"),List.empty,None,"kafka")
      val gdpr = BatchGdprETLModel("gdpr", List.empty,"string", List.empty,WriterModel.consoleWriter("test"),"lafka")

      val model2 = BatchJobModel("name2","description2","tester",true,10L,gdpr)
      batchJobBL.insert(model2)

      val model1 = BatchJobModel("name","description","tester",true,12L,etl)
      batchJobBL.insert(model1)



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

      val db = WaspMongoDB
      db.initializeDB()
      val waspDB = db.getDB()
      val batchJobBL = new BatchJobBLImp(waspDB)

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

      val db = WaspMongoDB
      db.initializeDB()
      val waspDB = db.getDB()
      val batchJobBL = new BatchJobBLImp(waspDB)

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
