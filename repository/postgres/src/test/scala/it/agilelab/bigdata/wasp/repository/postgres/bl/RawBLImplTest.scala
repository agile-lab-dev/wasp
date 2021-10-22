package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions}
import it.agilelab.bigdata.wasp.models.configuration.PostgresDBConfigModel
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDBImpl
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

trait RawBLImplTest {

  self : PostgresSuite =>

  val rawBL  = RawBLImpl(pgDB)

  it should "test RawBLImpl" in {

    rawBL.createTable()

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

    rawBL.createTable()

    val opt    = RawOptions("savemode", "format", Some(Map("extraoptions" -> "extravalue")), Some(List.empty))
    val model1 = RawModel("name", "uri", true, "schema", opt)
    rawBL.upsert(model1)

    rawBL.getByName(model1.name).get shouldBe model1

    val model2 = RawModel("name", "uri2", true, "schema2", opt)
    rawBL.upsert(model2)

    rawBL.getByName(model1.name).get shouldBe model2

  }

}
