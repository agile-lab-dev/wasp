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
	override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {
		val sparkConsumersMasterGuardianSingletonInfo = (
			Props(new SparkConsumersMasterGuardian(ConfigBL, SparkWriterFactoryDefault, KafkaReader)),
			WaspSystem.sparkConsumersMasterGuardianName,
			WaspSystem.sparkConsumersMasterGuardianSingletonManagerName,
			Seq(WaspSystem.sparkConsumersMasterGuardianRole)
		)
		
		val batchMasterGuardianSingletonInfo = (
			Props(new BatchMasterGuardian(ConfigBL, None, SparkWriterFactoryDefault)),
			WaspSystem.batchMasterGuardianName,
			WaspSystem.batchMasterGuardianSingletonManagerName,
			Seq(WaspSystem.batchMasterGuardianRole)
		)
		
		Seq(sparkConsumersMasterGuardianSingletonInfo, batchMasterGuardianSingletonInfo)
	}
	
	override def getNodeName: String = "consumers spark"
}