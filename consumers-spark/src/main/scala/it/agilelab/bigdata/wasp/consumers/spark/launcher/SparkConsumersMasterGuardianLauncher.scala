package it.agilelab.bigdata.wasp.consumers.spark.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.spark.readers.KafkaReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactoryDefault
import it.agilelab.bigdata.wasp.consumers.spark.{SparkConsumersMasterGuardian, SparkHolder}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher

/**
	* Launcher for the SparkConsumersMasterGuardian.
	*
	* @author Nicolò Bidotti
	*/
object SparkConsumersMasterGuardianLauncher extends ClusterSingletonLauncher {
	override def getSingletonProps: Props = {
		Props(new SparkConsumersMasterGuardian(ConfigBL, SparkWriterFactoryDefault, KafkaReader))
	}
	
	override def getSingletonName: String = SparkConsumersMasterGuardian.name
	
	override def getSingletonRole: String = SparkConsumersMasterGuardian.role
}
