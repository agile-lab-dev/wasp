package it.agilelab.bigdata.wasp.whitelabel.models.test

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import it.agilelab.bigdata.wasp.models.StrategyModel

/**
  * @author Nicolò Bidotti
  */
object TestStrategies {
  lazy val testKafkaHeaders = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaHeaders",
    configuration = None
  )

  lazy val testKafkaMetadata = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaMetadata",
    configuration = None
  )

  lazy val testKafkaMultitopicWriteJson = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaMultitopicWrite",
    configuration = Some(ConfigFactory.empty().withValue("format", ConfigValueFactory.fromAnyRef("json")).root().render())
  )

  lazy val testKafkaMultitopicWriteAvro = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaMultitopicWrite",
    configuration = Some(ConfigFactory.empty().withValue("format", ConfigValueFactory.fromAnyRef("avro")).root().render())
  )

  lazy val testKafkaPlaintext = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaPlaintext",
    configuration = None
  )

  lazy val testKafkaBinary = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaBinary",
    configuration = None
  )

  lazy val testAvroEncoderStrategy = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestAvroEncoderStrategy",
    configuration = None
  )

  lazy val testHttpHeaderStrategy = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestHttpHeaders",
    configuration = None
  )

  lazy val testHttpEnrichmentStrategy = StrategyModel(
    className = "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestHttpEnrichmentStrategy",
    configuration = None
  )

}
