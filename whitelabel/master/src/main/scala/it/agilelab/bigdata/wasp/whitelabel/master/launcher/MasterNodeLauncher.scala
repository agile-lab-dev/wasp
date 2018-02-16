package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import it.agilelab.bigdata.wasp.core.models.{PipegraphModel, TopicModel}
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import org.apache.commons.cli.CommandLine
import it.agilelab.bigdata.wasp.whitelabel.models.example._

object MasterNodeLauncher extends MasterNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    addExamplePipegraphs()
  }

  private def addExamplePipegraphs(): Unit = {
    waspDB.upsert[TopicModel](ExampleTopicModel.exampleTopic)
    waspDB.upsert[PipegraphModel](ExamplePipegraphModel.examplePipegraph)
  }
}