package it.agilelab.bigdata.wasp.core.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.{FlatSpec, Matchers}

class ConfigManagerSpec extends FlatSpec with Matchers {

  it should "load the basic Mongo configuration without errors" in {
    noException shouldBe thrownBy(ConfigManager.getMongoDBConfig)
  }

  it should "use the default database as the credential database" in {
    val mongoConf = ConfigManager.getMongoDBConfig
    mongoConf.credentialDb shouldBe mongoConf.databaseName
  }

  it should "be possible to override the credential database" in {
    val anotherConfigManager = new ConfigManager {
      override val conf: Config = ConfigFactory.load().getConfig("wasp").withValue("mongo.authentication-db", ConfigValueFactory.fromAnyRef("admin"))
    }
    anotherConfigManager.getMongoDBConfig.credentialDb shouldBe "admin"
  }

}
