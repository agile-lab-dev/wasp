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

    /* standalone applications should prefer waspDB.insertIfNotExist() instead of waspDB.upsert() */
    

    /* Example */

    /* Topic for Producers, Pipegraphs */
    waspDB.upsert[TopicModel](ExampleTopicModel.topic)

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](ExamplePipegraphModel.pipegraph)


    /** Test */

    /* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
    waspDB.upsert[TopicModel](TestTopicModel.json)
    waspDB.upsert[TopicModel](TestTopicModel.json2)
    waspDB.upsert[TopicModel](TestTopicModel.json3)
    waspDB.upsert[MultiTopicModel](TestTopicModel.jsonMultitopic)
    waspDB.upsert[TopicModel](TestTopicModel.json2ForKafkaHeaders)
    waspDB.upsert[TopicModel](TestTopicModel.jsonWithMetadata)
    waspDB.upsert[TopicModel](TestTopicModel.jsonCheckpoint)
    waspDB.upsert[TopicModel](TestTopicModel.avro)
    waspDB.upsert[TopicModel](TestTopicModel.avro2)
    waspDB.upsert[TopicModel](TestTopicModel.avro2ForKafkaHeaders)
    waspDB.upsert[TopicModel](TestTopicModel.avroCheckpoint)
    waspDB.upsert[IndexModel](TestIndexModel.solr)
    waspDB.upsert[IndexModel](TestIndexModel.elastic)
    waspDB.upsert[RawModel](TestRawModel.nested)  // used by TestPipegraphs.JSON.XYZ.hdfs
    waspDB.upsert[RawModel](TestRawModel.flat)    // used by TestBatchJobModels.FromHdfs.toConsole
    waspDB.upsert[KeyValueModel](TestKeyValueModel.hbase)
    waspDB.upsert[SqlSourceModel](TestSqlSouceModel.mySql)

    /* Producers */
    waspDB.upsert[ProducerModel](TestProducerModel.json)
    waspDB.upsert[ProducerModel](TestProducerModel.jsonWithMetadata)
    waspDB.upsert[ProducerModel](TestProducerModel.jsonCheckpoint)
    waspDB.upsert[ProducerModel](TestProducerModel.avro)
    waspDB.upsert[ProducerModel](TestProducerModel.avroCheckpoint)

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.consoleWithMetadata)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafkaHeaders)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafkaMultitopic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hdfs)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hbase)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.ERROR.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.CHECKPOINT.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Legacy.hdfs)

    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.kafkaHeaders)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.hdfs)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.hbase)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.ERROR.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.CHECKPOINT.console)
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