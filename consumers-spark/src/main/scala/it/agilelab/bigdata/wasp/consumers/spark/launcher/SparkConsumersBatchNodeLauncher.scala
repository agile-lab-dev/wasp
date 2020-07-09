package it.agilelab.bigdata.wasp.consumers.spark.launcher

import java.util.ServiceLoader

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.spark.batch.SparkConsumersBatchMasterGuardian
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactoryDefault
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule

import scala.collection.JavaConverters._

/**
	* Launcher for the SparkConsumersBatchMasterGuardian.
	* This trait is useful for who want extend the launcher
	*
	* @author Nicolò Bidotti
	*/
trait SparkConsumersBatchNodeLauncherTrait extends MultipleClusterSingletonsLauncher {

	var plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin] = Map()

	override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {
		val sparkConsumersBatchMasterGuardianSingletonInfo = (
			SparkConsumersBatchMasterGuardian.props(ConfigBL , SparkWriterFactoryDefault(plugins), plugins),
			WaspSystem.sparkConsumersBatchMasterGuardianName,
			WaspSystem.sparkConsumersBatchMasterGuardianSingletonManagerName,
			Seq(WaspSystem.sparkConsumersBatchMasterGuardianRole)
		)

		Seq(sparkConsumersBatchMasterGuardianSingletonInfo)
	}

	/**
		* Initialize the WASP plugins, this method is called after the wasp initialization and before getSingletonInfos
		* @param args command line arguments
		*/
	override def initializePlugins(args: Array[String]): Unit = {
		logger.info("Finding Spark consumers plugins")
		val pluginLoader: ServiceLoader[WaspConsumersSparkPlugin] = ServiceLoader.load[WaspConsumersSparkPlugin](classOf[WaspConsumersSparkPlugin])
		val pluginsList = pluginLoader.iterator().asScala.toList
		logger.info(s"Found ${pluginsList.size} plugins")
		logger.info("Initializing Spark consumers plugins")
		plugins = pluginsList.map({
			plugin => {
				logger.info(s"Initializing Spark consumers plugin ${plugin.getClass.getSimpleName} for datastore product ${plugin.datastoreProduct}")
				plugin.initialize(waspDB)
				// You cannot have two plugin with the same name
				(plugin.datastoreProduct, plugin)
			}
		}).toMap

		logger.info(s"Initialized all Spark consumers plugins")
	}

	override def validateConfigs(pluginsValidationRules: Seq[ValidationRule] = Seq()): Unit = {
		val pluginsValidationRules = plugins.flatMap(plugin => plugin._2.getValidationRules).toSeq

		super.validateConfigs(pluginsValidationRules)
	}

	override def getNodeName: String = "batch consumers spark"
}

/**
	* Create the main static method to run
	*
	*/
object SparkConsumersBatchNodeLauncher extends SparkConsumersBatchNodeLauncherTrait