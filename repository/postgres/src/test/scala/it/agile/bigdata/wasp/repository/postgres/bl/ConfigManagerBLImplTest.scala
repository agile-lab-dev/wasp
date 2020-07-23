package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.utils.PostgresSuite
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.models.configuration.{CompilerConfigModel, JdbcConfigModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

class ConfigManagerBLImplTest extends PostgresSuite with JsonSupport{

  private val bl = ConfigManagerBLImpl(pgDB)

  it should "test Config manager for postgres" in {
    bl.createTable()
    val jdbcConfigModel =JdbcConfigModel(Map.empty,ConfigManager.jdbcConfigName)
    bl.retrieveConf[JdbcConfigModel](jdbcConfigModel,jdbcConfigModel.name).get shouldBe jdbcConfigModel
    bl.getByName[JdbcConfigModel](jdbcConfigModel.name).get shouldBe jdbcConfigModel

    bl.getByName[JdbcConfigModel]("name_2").isEmpty shouldBe true

    val compilerConfigModel = CompilerConfigModel(4,ConfigManager.compilerConfigName)
    val compilerConfigModelBis = CompilerConfigModel(8,ConfigManager.compilerConfigName)

    bl.retrieveConf[CompilerConfigModel](compilerConfigModel,compilerConfigModel.name).get shouldBe compilerConfigModel
    bl.retrieveConf[CompilerConfigModel](compilerConfigModelBis,compilerConfigModelBis.name).get shouldBe compilerConfigModel



    bl.retrieveDBConfig() should contain theSameElementsAs Seq(jdbcConfigModel.toJson.toString(),compilerConfigModel.toJson.toString())

  }

}
