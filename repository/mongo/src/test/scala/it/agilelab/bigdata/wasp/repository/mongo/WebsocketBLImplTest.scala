package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.mongo.bl.WebsocketBLImp
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class WebsocketBLImplTest extends FlatSpec with Matchers{

  it should "test WebsocketBLImpl for Mongo" in {

    val model1 = WebsocketModel("name","host","9999","resources",None)
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new WebsocketBLImp(waspDB)

    bl.persist(model1)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName("XXX").isEmpty shouldBe true

  }
}
