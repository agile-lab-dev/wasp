package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import it.agilelab.bigdata.wasp.core.models.{IndexModel, PipegraphModel, ProducerModel, TopicModel}
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import org.apache.commons.cli.CommandLine
import it.agilelab.bigdata.wasp.whitelabel.models.example._
import it.agilelab.bigdata.wasp.whitelabel.models.test.{TestIndexModel, TestPipegraphs, TestProducerModel, TestTopicModel}

object MasterNodeLauncher extends MasterNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    addExamplePipegraphs()
  }

  private def addExamplePipegraphs(): Unit = {

    /* Example */
    waspDB.upsert[TopicModel](ExampleTopicModel())
    waspDB.upsert[PipegraphModel](ExamplePipegraphModel())


    /* Test */
    waspDB.upsert[TopicModel](TestTopicModel.testJsonTopic)
    waspDB.upsert[TopicModel](TestTopicModel.testAvroTopic)
    waspDB.upsert[IndexModel](TestIndexModel())

    waspDB.upsert[ProducerModel](TestProducerModel.JSON())
    waspDB.upsert[ProducerModel](TestProducerModel.AVRO())

    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.solr)

    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.solr)
  }
}