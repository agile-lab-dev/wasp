package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.{KeyValueModel, KeyValueOption}
import it.agilelab.bigdata.wasp.repository.postgres.utils.PostgresSuite

trait KeyValueBLImplTest {
  self : PostgresSuite =>


  lazy val keyValueBL = KeyValueBLImpl(pgDB)

    it should "test keyValueBL" in {

      keyValueBL.createTable()

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

      keyValueBL.createTable()

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
