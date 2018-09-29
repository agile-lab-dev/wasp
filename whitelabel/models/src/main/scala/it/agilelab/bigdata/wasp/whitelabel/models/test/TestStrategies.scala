package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.StrategyModel

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
}
