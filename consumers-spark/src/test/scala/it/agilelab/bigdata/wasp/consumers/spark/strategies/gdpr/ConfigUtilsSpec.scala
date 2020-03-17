package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HBaseDeletionConfig._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.utils.ConfigUtils
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, TryValues}

class ConfigUtilsSpec extends FlatSpec with Matchers with TryValues with BeforeAndAfterEach with Logging {

  it should "parse the keys correctly" in {
    val keys = Seq("k1", "k2")
    val correlationId = "corrId1"
    val stringConfig =
      s"""
        |{ "$KV_CONF_KEY" { "$KEYS_TO_DELETE_KEY" = [${keys.mkString(",")}], "$CORRELATION_ID_KEY" = "$correlationId" } }
        |""".stripMargin

    println(stringConfig)

    val rootConfig = ConfigFactory.parseString(stringConfig)
    val config = ConfigUtils.getOptionalConfig(rootConfig, KV_CONF_KEY)

    val keysFound = ConfigUtils.keysToDelete(Seq.empty, config, KEYS_TO_DELETE_KEY, CORRELATION_ID_KEY)

    keysFound should contain theSameElementsAs Seq(KeyWithCorrelation("k1", correlationId), KeyWithCorrelation("k2", correlationId))
  }

}
