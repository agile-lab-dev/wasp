package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.launcher

import it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersStreamingNodeLauncherTrait
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Protocol
import it.agilelab.bigdata.wasp.core.WaspSystem
import org.apache.commons.cli.CommandLine

object SparkConsumersStreamingNodeLauncher extends SparkConsumersStreamingNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
  }
}