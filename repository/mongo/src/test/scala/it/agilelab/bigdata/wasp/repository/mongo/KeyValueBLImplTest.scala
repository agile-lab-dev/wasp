package it.agilelab.bigdata.wasp.repository.mongo

import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import it.agilelab.bigdata.wasp.models.{KeyValueModel, KeyValueOption}
import it.agilelab.bigdata.wasp.repository.mongo.bl.KeyValueBLImp
import org.scalatest.{BeforeAndAfter, DoNotDiscover, FlatSpec, FunSuite, Matchers}

@DoNotDiscover
class KeyValueBLImplTest extends FlatSpec with Matchers{

  it should "test keyValueBL for Mongo" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val keyValueBL = new KeyValueBLImp(waspDB)


    val kOpts = Seq(KeyValueOption("key","value"))
    val model1 = KeyValueModel("name", "tableCatalog", Some("dataFrameSchema"), Some(kOpts), false, None)
    val model2 = KeyValueModel("name2", "tableCatalog2", Some("dataFrameSchema2"), Some(kOpts), false, None)

    keyValueBL.persist(model1)

    keyValueBL.persist(model2)

    val list = keyValueBL.getAll

    list.size shouldBe 2
    list should contain theSameElementsAs Seq(model1, model2)

    keyValueBL.getByName(model1.name).get shouldBe model1
    keyValueBL.getByName(model2.name).get shouldBe model2
    keyValueBL.getByName("XXXX").isEmpty shouldBe true

    keyValueBL.getAll.size shouldBe 2

  }

  it should "test keyValueBL upsert" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val keyValueBL = new KeyValueBLImp(waspDB)


    val kOpts = Seq(KeyValueOption("key","value"))
    val model3 = KeyValueModel("name3", "tableCatalog", Some("dataFrameSchema"), Some(kOpts), false, None)
    val model4 = KeyValueModel("name3", "tableCatalog2", Some("dataFrameSchema2"), Some(kOpts), false, None)

    keyValueBL.upsert(model3)

    keyValueBL.getByName(model3.name).get shouldBe model3

    keyValueBL.upsert(model4)

    keyValueBL.getByName(model3.name).get shouldBe model4

    keyValueBL.getAll.filter(model => model.name == model3.name).head shouldBe model4

  }
}
