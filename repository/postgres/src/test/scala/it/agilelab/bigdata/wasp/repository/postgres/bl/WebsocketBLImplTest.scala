package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

trait WebsocketBLImplTest {
  self : PostgresSuite =>

  private lazy val bl = WebsocketBLImpl(pgDB)

  it should "test WebsocketBLImpl" in {

    val model1 = WebsocketModel("name","host","9999","resources",None)

    bl.createTable()
    bl.persist(model1)

    bl.getByName(model1.name).get shouldBe model1
    bl.getByName("XXX").isEmpty shouldBe true

  }

}
