package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import it.agilelab.bigdata.wasp.core.SystemPipegraphs
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import org.apache.commons.cli.CommandLine
import org.mongodb.scala.bson.BsonObjectId

object WhitelabelMasterNodeLauncher extends MasterNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    addExamplePipegraph()
  }


  def addExamplePipegraph(): Unit = {
    val db = WaspDB.getDB

    db.upsert(ExamplePipegraphs.exampleTopic)
    db.upsert(ExamplePipegraphs.examplePipegraph)
  }

}


