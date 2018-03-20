package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import org.apache.commons.cli.CommandLine
import it.agilelab.bigdata.wasp.whitelabel.models.example._
import it.agilelab.bigdata.wasp.whitelabel.models.test._

object MasterNodeLauncher extends MasterNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    addExamplePipegraphs()
  }

  private def addExamplePipegraphs(): Unit = {

    /* Example */

    /* Topic for Producers, Pipegraphs */
    waspDB.upsert[TopicModel](ExampleTopicModel.topic)

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](ExamplePipegraphModel.pipegraph)


    /** Test */

    /* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
    waspDB.upsert[TopicModel](TestTopicModel.json)
    waspDB.upsert[TopicModel](TestTopicModel.json2)
    waspDB.upsert[TopicModel](TestTopicModel.avro)
    waspDB.upsert[TopicModel](TestTopicModel.avro2)
    waspDB.upsert[IndexModel](TestIndexModel.solr)
    waspDB.upsert[IndexModel](TestIndexModel.elastic)
    waspDB.upsert[RawModel](TestRawModel.nested)  // used by TestPipegraphs.JSON.XYZ.hdfs
    waspDB.upsert[RawModel](TestRawModel.flat)    // used by TestBatchJobModels.FromHdfs.toConsole
    waspDB.upsert[KeyValueModel](TestKeyValueModel.simple)                        // TODO revise
    waspDB.upsert[SqlSourceModel](TestSqlSouceModel.mySql)

    /* Producers */
    waspDB.upsert[ProducerModel](TestProducerModel.json)
    waspDB.upsert[ProducerModel](TestProducerModel.avro)

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hdfs)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hbase)           // TODO revise
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.ERROR.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.hdfs)

    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.hdfs)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.ERROR.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.hdfs)

    /* BatchJobs */
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromElastic.toHdfsNested)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromSolr.toHdfsFlat)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromHdfs.flatToConsole)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromHdfs.nestedToConsole)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromJdbc.mySqlToConsole)
  }
}