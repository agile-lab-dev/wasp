package it.agilelab.bigdata.wasp.repository.mongo

import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions}
import it.agilelab.bigdata.wasp.repository.mongo.bl.RawBLImp
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, DoNotDiscover, FlatSpec, FunSuite, Matchers}

@DoNotDiscover
class RawBLImplTest extends FlatSpec with Matchers{

  it should "test RawBLImpl for Mongo" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val rawBL = new RawBLImp(waspDB)

    val opt    = RawOptions("savemode", "format", Some(Map("extraoptions" -> "extravalue")), Some(List.empty))
    val model1 = RawModel("name", "uri", true, "schema", opt)
    rawBL.persist(model1)

    val model2 = RawModel("name2", "uri2", true, "schema2", opt)
    rawBL.persist(model2)

    val list = rawBL.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1, model2)

    rawBL.getByName(model1.name).get shouldBe model1
    rawBL.getByName(model2.name).get shouldBe model2
    rawBL.getByName("XXXX").isEmpty shouldBe true

  }

  it should "test rawBL upsert" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val rawBL = new RawBLImp(waspDB)

    val opt    = RawOptions("savemode", "format", Some(Map("extraoptions" -> "extravalue")), Some(List.empty))
    val model1 = RawModel("name", "uri", true, "schema", opt)
    rawBL.upsert(model1)

    rawBL.getByName(model1.name).get shouldBe model1

    val model2 = RawModel("name", "uri2", true, "schema2", opt)
    rawBL.upsert(model2)

    rawBL.getByName(model1.name).get shouldBe model2

  }

}
