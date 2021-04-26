package it.agilelab.bigdata.wasp.whitelabel.launcher

import akka.actor.Props
import akka.http.scaladsl.server.Directives.reject
import akka.http.scaladsl.server.Route
import it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersStreamingNodeLauncherTrait
import it.agilelab.bigdata.wasp.core.AroundLaunch
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.WaspConfiguration
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import it.agilelab.bigdata.wasp.producers.launcher.ProducersNodeLauncherTrait
import it.agilelab.bigdata.wasp.whitelabel.master.launcher.MasterNodeLauncher
import org.apache.commons.cli.CommandLine

object SingleNodeLauncher extends MultipleClusterSingletonsLauncher with WaspConfiguration {

  private val launchers: List[MultipleClusterSingletonsLauncher with AroundLaunch] = List(
    new MasterNodeLauncherTrait                  {},
    new ProducersNodeLauncherTrait               {},
    new SparkConsumersStreamingNodeLauncherTrait {}
  )

  override def initializePlugins(args: Array[String]): Unit =
    launchers.foreach(_.initializePlugins(args))

  override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] =
    launchers.flatMap(_.getSingletonInfos)

  override def getNodeName: String = "master_producers_spark-streaming"

  override def launch(commandLine: CommandLine): Unit = {
    launchers.foreach(_.beforeLaunch())
    super.launch(commandLine)
    launchers.foreach(_.afterLaunch())
    MasterNodeLauncher.addExampleRegisterAvroSchema()
    MasterNodeLauncher.addExamplePipegraphs()
  }

  override def validateConfigs(pluginsValidationRules: Seq[ValidationRule]): Unit = {
    super.validateConfigs(pluginsValidationRules)
    launchers.foreach(_.validateConfigs(pluginsValidationRules))
  }

  def additionalRoutes(): Route = reject
}
