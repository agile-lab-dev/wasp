package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.genericProduct
import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models.GenericModel
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite
import org.mongodb.scala.bson.BsonDocument

trait GenericBLImplTest {
  self : PostgresSuite =>

  private val bl = GenericBLImpl(pgDB)

  it should "test GenericBLImpl" in {
    bl.dropTable()
    bl.createTable()

    val model1 = GenericModel(
      "model1",
      BsonDocument("""{"value":"generic-value"}"""),
      product = genericProduct
    )

    val model1Bis = model1.copy(product = GenericProduct("generic-kind1", None))
    val model2 = model1Bis.copy(name = "model2")
    val model3 = model1.copy(name = "model3", value = BsonDocument("""{"value":"generic-value3"}"""))

    bl.persist(model1)
    bl.getByName(model1.name).get shouldBe model1

    bl.upsert(model1Bis)
    bl.getByName(model1Bis.name).get shouldBe model1Bis

    bl.upsert(model2)
    bl.getByName(model2.name).get shouldBe model2

    bl.upsert(model3)
    bl.getByName(model3.name).get shouldBe model3

    bl.getAll().foreach(println)
    bl.getAll().size shouldBe 3

  }
}
