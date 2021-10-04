package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.{HttpCompression, HttpModel}
import it.agilelab.bigdata.wasp.repository.mongo.bl.HttpBlImpl
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class HttpBLImplTest extends FlatSpec with Matchers{


  it should "test HttpBLImpl" in {

    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new HttpBlImpl(waspDB)

    val model1 = HttpModel(
      "model1",
      "http://127.0.0.1:8080",
      "POST",
      Some("header"),
      List("Value1", "Value2"),
      HttpCompression.Disabled,
      "text/plain",
      false)

    val model1Bis = model1.copy(url = "http://127.0.0.1:8181")
    val model2 = model1Bis .copy(name = "model2")
    val model3 = model1.copy(name= "model3", method = "GET")

    bl.persist(model1)
    bl.getByName(model1.name).get shouldBe model1

    bl.insertIfNotExists(model1Bis )
    bl.getByName(model1Bis .name).get shouldBe model1

    bl.upsert(model1Bis)
    bl.getByName(model1Bis.name).get shouldBe model1Bis

    bl.insertIfNotExists(model2)
    bl.getByName(model2.name).get shouldBe model2

    bl.upsert(model3)
    bl.getByName(model3.name).get shouldBe model3

    bl.getAll().foreach(println)
    bl.getAll().size shouldBe 3

  }

}
