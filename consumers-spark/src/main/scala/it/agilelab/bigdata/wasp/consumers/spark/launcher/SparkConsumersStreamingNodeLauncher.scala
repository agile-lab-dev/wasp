package it.agilelab.bigdata.wasp.consumers.spark.launcher

import java.util.ServiceLoader

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka.KafkaStructuredReader
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian.ChildCreator
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactoryDefault
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import org.apache.commons.cli.CommandLine

import scala.collection.JavaConverters._

/**
	* Launcher for the SparkConsumersStreamingMasterGuardian.
	* This trait is useful for who want extend the launcher
	*
	* @author Nicolò Bidotti
	*/
trait SparkConsumersStreamingNodeLauncherTrait extends MultipleClusterSingletonsLauncher {

	var plugins: Map[String, WaspConsumersSparkPlugin] = Map()

	override def launch(commandLine: CommandLine): Unit = {
		SparkSingletons.initializeSpark(ConfigManager.getSparkStreamingConfig)
		SparkSingletons.initializeSparkStreaming(ConfigManager.getSparkStreamingConfig)

		super.launch(commandLine)
	}

	override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {

		import scala.concurrent.duration._


		val childrenCreator: ChildCreator = SparkConsumersStreamingMasterGuardian.defaultChildCreator(
			KafkaStructuredReader,
			plugins,
			SparkSingletons.getSparkSession,
			SparkWriterFactoryDefault(plugins),
			5.seconds,
			5.seconds,
			_ => PipegraphGuardian.Retry,
			ConfigBL)

		val watchdog = if(ConfigManager.getSparkStreamingConfig.driver.killDriverProcessIfSparkContextStops) {

      SparkConsumersStreamingMasterGuardian.exitingWatchdogCreator(SparkSingletons.getSparkSession.sparkContext,
      -1)

    }else {
      SparkConsumersStreamingMasterGuardian.doNothingWatchdogCreator(SparkSingletons.getSparkSession.sparkContext)
    }

		val masterGuardianProps = SparkConsumersStreamingMasterGuardian.props(
			ConfigBL.pipegraphBL,
			childrenCreator,
      watchdog,
			5.seconds)


		val sparkConsumersStreamingMasterGuardianSingletonInfo = (
			masterGuardianProps,
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

	override def validateConfigs(pluginsValidationRules: Seq[ValidationRule] = Seq()): Unit = {
		val pluginsValidationRules = plugins.flatMap(plugin => plugin._2.getValidationRules).toSeq

		super.validateConfigs(pluginsValidationRules)
	}

	override def getNodeName: String = "streaming consumers spark"
}

/**
	* Create the main static method to run
	*
	*/
object SparkConsumersStreamingNodeLauncher extends SparkConsumersStreamingNodeLauncherTrait