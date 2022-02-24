package it.agilelab.bigdata.wasp.consumers.spark.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.spark.SparkSingletons
import it.agilelab.bigdata.wasp.consumers.spark.plugins.WaspConsumersSparkPlugin
import it.agilelab.bigdata.wasp.consumers.spark.readers.PluginBasedSparkReaderFactory
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.collaborator.CollaboratorActor
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SparkConsumersStreamingMasterGuardian.ChildCreator
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.{
  SchedulingStrategyFactory,
  SparkConsumersStreamingMasterGuardian
}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph.PipegraphGuardian
import it.agilelab.bigdata.wasp.consumers.spark.writers.SparkWriterFactoryDefault
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.core.{AroundLaunch, WaspSystem}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import org.apache.commons.cli.CommandLine

import java.util.ServiceLoader
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/**
  * Launcher for the SparkConsumersStreamingMasterGuardian.
  * This trait is useful for who want extend the launcher
  *
  * @author Nicolò Bidotti
  */
trait SparkConsumersStreamingNodeLauncherTrait extends MultipleClusterSingletonsLauncher with AroundLaunch {

  var plugins: Map[DatastoreProduct, WaspConsumersSparkPlugin] = Map()

  def beforeLaunch(): Unit = {
    SparkSingletons.initializeSpark(
      ConfigManager.getSparkStreamingConfig,
      ConfigManager.getTelemetryConfig,
      ConfigManager.getKafkaConfig
    )
  }

  def afterLaunch(): Unit = ()

  override def launch(commandLine: CommandLine): Unit = {
    beforeLaunch()
    super.launch(commandLine)
    afterLaunch()
  }

  override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {

    import scala.concurrent.duration._

    val childrenCreator: ChildCreator = SparkConsumersStreamingMasterGuardian.defaultChildCreator(
      SparkSingletons.getSparkSession,
      new PluginBasedSparkReaderFactory(plugins),
      SparkWriterFactoryDefault(plugins),
      5.seconds,
      5.seconds,
      _ => PipegraphGuardian.Retry,
      ConfigBL
    )

    val collaborator = CollaboratorActor.props(WaspSystem.sparkConsumersStreamingMasterGuardian, childrenCreator)

    logger.error("Starting collaborator")
    WaspSystem.actorSystem.actorOf(collaborator, "collaborator")
    logger.error("Collaborator started")

    val watchdog = if (ConfigManager.getSparkStreamingConfig.driver.killDriverProcessIfSparkContextStops) {
      SparkConsumersStreamingMasterGuardian.exitingWatchdogCreator(SparkSingletons.getSparkSession.sparkContext, -1)
    } else {
      SparkConsumersStreamingMasterGuardian.doNothingWatchdogCreator(SparkSingletons.getSparkSession.sparkContext)
    }

    val schedulingStrategyFactory: SchedulingStrategyFactory =
      try {
        val strategy = Class
          .forName(ConfigManager.getSparkStreamingConfig.schedulingStrategy.factoryClass)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[SchedulingStrategyFactory]
        strategy.inform(ConfigManager.getSparkStreamingConfig.schedulingStrategy.factoryParams)
      } catch {
        case e: Exception =>
          logger.error(
            s"Could not instantiate ${ConfigManager.getSparkStreamingConfig.schedulingStrategy.factoryClass}",
            e
          )
          throw e
      }

    val masterGuardianProps = SparkConsumersStreamingMasterGuardian.props(
      ConfigBL.pipegraphBL,
      watchdog,
      "collaborator",
      5.seconds,
      FiniteDuration(5, TimeUnit.SECONDS),
      schedulingStrategy = schedulingStrategyFactory
    )

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
    *
    * @param args command line arguments
    */
  override def initializePlugins(args: Array[String]): Unit = {
    logger.info("Finding Spark consumers plugins")
    val pluginLoader: ServiceLoader[WaspConsumersSparkPlugin] =
      ServiceLoader.load[WaspConsumersSparkPlugin](classOf[WaspConsumersSparkPlugin])
    val pluginsList = pluginLoader.iterator().asScala.toList
    logger.info(s"Found ${pluginsList.size} plugins")
    logger.info("Initializing consumers Spark plugins")
    plugins = pluginsList.map { plugin =>
      logger.info(
        s"Initializing Spark consumers plugin ${plugin.getClass.getSimpleName} for datastore product ${plugin.datastoreProduct}"
      )
      plugin.initialize(waspDB)
      // You cannot have two plugin with the same name
      plugin.datastoreProduct -> plugin
    }.toMap

    logger.info(s"Initialized all Spark consumers plugins")
  }

  override def validateConfigs(pluginsValidationRules: Seq[ValidationRule] = Seq()): Unit = {
    val pluginsValidationRules = plugins.flatMap(plugin => plugin._2.getValidationRules).toSeq

    super.validateConfigs(pluginsValidationRules)
  }

  override def getNodeName: String = "streaming consumers spark"

  override protected def shouldDropDb(commandLine: CommandLine): Boolean = false
}

/**
  * Create the main static method to run
  *
  */
object SparkConsumersStreamingNodeLauncher extends SparkConsumersStreamingNodeLauncherTrait
