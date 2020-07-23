package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.utils.PostgresSuite
import it.agilelab.bigdata.wasp.models.WebsocketModel

class WebsocketBLImplTest extends PostgresSuite{

  val bl = WebsocketBLImpl(pgDB)

  it should "test WebsocketBLImpl" in {

    val model1 = WebsocketModel("name","host","9999","resources",None)

    bl.createTable()
    bl.persist(model1)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName("XXX").isEmpty shouldBe true

  }

}
