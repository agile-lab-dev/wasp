package it.agilelab.bigdata.wasp.consumers.spark.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.spark.batch.BatchMasterGuardian
import it.agilelab.bigdata.wasp.consumers.spark.readers.KafkaReader
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactoryDefault
import it.agilelab.bigdata.wasp.consumers.spark.SparkConsumersMasterGuardian
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher

/**
	* Launcher for the SparkConsumersMasterGuardian and BatchMasterGuardian.
	*
	* @author Nicolò Bidotti
	*/
object SparkConsumersAndBatchMasterGuardianLauncher extends MultipleClusterSingletonsLauncher {
	override def getSingletonInfos: Seq[(Props, String, Seq[String])] = {
		Seq(
			(Props(new SparkConsumersMasterGuardian(ConfigBL, SparkWriterFactoryDefault, KafkaReader)), WaspSystem.sparkConsumersMasterGuardianName, Seq(WaspSystem.sparkConsumersMasterGuardianRole)),
			(Props(new BatchMasterGuardian(ConfigBL, None, SparkWriterFactoryDefault)), WaspSystem.batchMasterGuardianName, Seq(WaspSystem.batchMasterGuardianRole))
		)
	}
}