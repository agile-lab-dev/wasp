package it.agilelab.bigdata.wasp.consumers.spark.launcher

import java.util.ServiceLoader

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.{KafkaReader, KafkaStructuredReader}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.guardian.master.SparkConsumersStreamingMasterGuardian
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactoryDefault
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher

import scala.collection.JavaConverters._

/**
	* Launcher for the SparkConsumersStreamingMasterGuardian.
	* This trait is useful for who want extend the launcher
	*
	* @author Nicolò Bidotti
	*/
trait SparkConsumersStreamingNodeLauncherTrait extends MultipleClusterSingletonsLauncher {

	var plugins: Map[String, WaspConsumersSparkPlugin] = Map()

	override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {
		val sparkConsumersStreamingMasterGuardianSingletonInfo = (
			SparkConsumersStreamingMasterGuardian.props(ConfigBL.pipegraphBL),
      WaspSystem.sparkConsumersStreamingMasterGuardianName,
			WaspSystem.sparkConsumersStreamingMasterGuardianSingletonManagerName,
			Seq(WaspSystem.sparkConsumersStreamingMasterGuardianRole)
		)

		Seq(sparkConsumersStreamingMasterGuardianSingletonInfo)
	}

	/**
		* Initialize the WASP plugins, this method is called after the wasp initialization and before getSingletonInfos
		* @param args command line arguments
		*/
	override def initializePlugins(args: Array[String]): Unit = {
		logger.info("Finding WASP streaming consumers spark plugins")
		val pluginLoader: ServiceLoader[WaspConsumersSparkPlugin] = ServiceLoader.load[WaspConsumersSparkPlugin](classOf[WaspConsumersSparkPlugin])
		val pluginsList = pluginLoader.iterator().asScala.toList
		logger.info(s"Found ${pluginsList.size} plugins")
		logger.info("Initializing streaming consumers spark plugins")
		plugins = pluginsList.map({
			plugin => {
				logger.info(s"Initializing streaming consumers spark plugin ${plugin.getClass.getSimpleName} with this type: ${plugin.pluginType}")
				plugin.initialize(waspDB)
				// You cannot have two plugin with the same name
				(plugin.pluginType, plugin)
			}
		}).toMap

		logger.info(s"Initialized all streaming consumers spark plugins")
	}

	override def getNodeName: String = "streaming consumers spark"
}

/**
	* Create the main static method to run
	*
	*/
object SparkConsumersStreamingNodeLauncher extends SparkConsumersStreamingNodeLauncherTrait