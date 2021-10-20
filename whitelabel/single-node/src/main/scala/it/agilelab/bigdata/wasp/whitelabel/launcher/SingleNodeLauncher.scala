package it.agilelab.bigdata.wasp.whitelabel.launcher

import akka.actor.Props
import akka.http.scaladsl.server.Directives.reject
import akka.http.scaladsl.server.Route
import it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersStreamingNodeLauncherTrait
import it.agilelab.bigdata.wasp.core.{AroundLaunch, WaspSystem}
import it.agilelab.bigdata.wasp.core.launcher.{MasterCommandLineOptions, MultipleClusterSingletonsLauncher}
import it.agilelab.bigdata.wasp.core.models.configuration.ValidationRule
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspConfiguration}
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import it.agilelab.bigdata.wasp.producers.launcher.ProducersNodeLauncherTrait
import it.agilelab.bigdata.wasp.producers.metrics.kafka.KafkaCheckOffsetsGuardian
import it.agilelab.bigdata.wasp.whitelabel.master.launcher.MasterNodeLauncher
import org.apache.commons.cli
import org.apache.commons.cli.CommandLine

object SingleNodeLauncher extends MultipleClusterSingletonsLauncher with WaspConfiguration {

  def locations(clz: Class[_], classLoader:ClassLoader): List[String] = {
    val enums = classLoader.getResources(clz.getCanonicalName.replace(".","/") + ".class")
    val list = List.newBuilder[String]
    while (enums.hasMoreElements){
      list. += (enums.nextElement().toString)
    }
    list.result()
  }

  def locations(clz: Class[_]): List[String] = {
    locations(clz, getClass.getClassLoader)
  }

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
    locations(classOf[org.apache.commons.cli.Options])
    launchers.foreach(_.beforeLaunch())
    super.launch(commandLine)
    launchers.foreach(_.afterLaunch())
    MasterNodeLauncher.addExampleRegisterAvroSchema()
    MasterNodeLauncher.addExamplePipegraphs()
    WaspSystem.actorSystem.actorOf(
      KafkaCheckOffsetsGuardian.props(ConfigManager.getKafkaConfig),
      KafkaCheckOffsetsGuardian.name
    )
  }

  override def validateConfigs(pluginsValidationRules: Seq[ValidationRule]): Unit = {
    super.validateConfigs(pluginsValidationRules)
    launchers.foreach(_.validateConfigs(pluginsValidationRules))
  }

  def additionalRoutes(): Route = reject

  override def getOptions: Seq[cli.Option] =
    (super.getOptions ++ launchers.flatMap(_.getOptions)).distinct

  override protected def shouldDropDb(commandLine: CommandLine): Boolean =
    commandLine.hasOption(MasterCommandLineOptions.dropDb.getOpt)
}
