package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import it.agilelab.bigdata.wasp.whitelabel.models.example._
import it.agilelab.bigdata.wasp.whitelabel.models.test._
import it.agilelab.darwin.manager.{AvroSchemaManager, AvroSchemaManagerFactory}
import org.apache.avro.Schema
import org.apache.commons.cli.CommandLine

import scala.collection.JavaConversions._

object MasterNodeLauncher extends MasterNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    addExamplePipegraphs()
    addExampleRegisterAvroSchema()
  }

  /** Add schema to AvroSchemaManager.
    *
    * @return [[Seq[(Key, Schema)]]
    */
  private def addExampleRegisterAvroSchema(): Unit = {
    val schemas: Seq[Schema] = Seq(AvroSchema[TopicAvro_v1], AvroSchema[TopicAvro_v2], new Schema.Parser().parse(TestTopicModel.avro.schema.toJson))
    val configAvroSchemaManager = ConfigManager.getAvroSchemaManagerConfig
    AvroSchemaManagerFactory.initialize(configAvroSchemaManager).registerAll(schemas)
  }

  private def addExamplePipegraphs(): Unit = {

    /* standalone applications should prefer waspDB.insertIfNotExist() instead of waspDB.upsert() */

    /*
     * =================================================================================================================
     * Examples
     * =================================================================================================================
     */

    /* Topic for Producers, Pipegraphs */
    waspDB.upsert[TopicModel](ExampleTopicModel.topic)

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](ExamplePipegraphModel.pipegraph)

    /*
     * =================================================================================================================
     * Tests
     * =================================================================================================================
     */

    /* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
    waspDB.upsert[TopicModel](TestTopicModel.json)
    waspDB.upsert[TopicModel](TestTopicModel.json2)
    waspDB.upsert[TopicModel](TestTopicModel.json3)
    waspDB.upsert[TopicModel](TestTopicModel.json4)
    waspDB.upsert[TopicModel](TestTopicModel.json5)
    waspDB.upsert[TopicModel](TestTopicModel.json6)
    waspDB.upsert[MultiTopicModel](TestTopicModel.jsonMultitopicRead)
    waspDB.upsert[MultiTopicModel](TestTopicModel.jsonMultitopicWrite)
    waspDB.upsert[TopicModel](TestTopicModel.json2ForKafkaHeaders)
    waspDB.upsert[TopicModel](TestTopicModel.jsonWithMetadata)
    waspDB.upsert[TopicModel](TestTopicModel.jsonCheckpoint)
    waspDB.upsert[TopicModel](TestTopicModel.avro)
    waspDB.upsert[TopicModel](TestTopicModel.avro2)
    waspDB.upsert[TopicModel](TestTopicModel.avro3)
    waspDB.upsert[TopicModel](TestTopicModel.avro4)
    waspDB.upsert[TopicModel](TestTopicModel.avro5)
    waspDB.upsert[MultiTopicModel](TestTopicModel.avroMultitopicRead)
    waspDB.upsert[MultiTopicModel](TestTopicModel.avroMultitopicWrite)
    waspDB.upsert[TopicModel](TestTopicModel.avro2ForKafkaHeaders)
    waspDB.upsert[TopicModel](TestTopicModel.avroCheckpoint)
    waspDB.upsert[TopicModel](TestTopicModel.plaintext1)
    waspDB.upsert[TopicModel](TestTopicModel.plaintext2)
    waspDB.upsert[MultiTopicModel](TestTopicModel.plaintextMultitopic)
    waspDB.upsert[TopicModel](TestTopicModel.binary1)
    waspDB.upsert[TopicModel](TestTopicModel.binary2)
    waspDB.upsert[MultiTopicModel](TestTopicModel.binaryMultitopic)
    waspDB.upsert[IndexModel](TestIndexModel.solr)
    waspDB.upsert[IndexModel](TestIndexModel.elastic)
    waspDB.upsert[RawModel](TestRawModel.nested) // used by TestPipegraphs.JSON.XYZ.hdfs
    waspDB.upsert[RawModel](TestRawModel.flat) // used by TestBatchJobModels.FromHdfs.toConsole
    waspDB.upsert[RawModel](TestRawModel.text) // used by TestBatchJobModels.FromHdfs.toConsole
    waspDB.upsert[KeyValueModel](TestKeyValueModel.hbase)
    waspDB.upsert[KeyValueModel](TestKeyValueModel.hbaseMultipleClusteringKeyValueModel)
    waspDB.upsert[SqlSourceModel](TestSqlSouceModel.mySql)
    waspDB.upsert[DocumentModel](TestMongoModel.writeToMongo)
    waspDB.upsert[RawModel](TestGdprBatchJobModels.outputRawModel)
    waspDB.upsert[RawModel](TestGdprBatchJobModels.inputRawModel)
    waspDB.upsert[RawModel](TestGdprBatchJobModels.dataRawModel)

    /* Producers */
    waspDB.upsert[ProducerModel](TestProducerModel.json)
    waspDB.upsert[ProducerModel](TestProducerModel.jsonWithMetadata)
    waspDB.upsert[ProducerModel](TestProducerModel.jsonCheckpoint)
    waspDB.upsert[ProducerModel](TestProducerModel.avro)
    waspDB.upsert[ProducerModel](TestProducerModel.avroCheckpoint)
    waspDB.upsert[ProducerModel](TestProducerModel.jsonHbaseMultipleClustering)

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.mongo)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.consoleWithMetadata)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafkaHeaders)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafkaMultitopicRead)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.kafkaMultitopicWrite)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hdfs)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hbase)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.hbaseMultipleClustering)
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
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.kafkaMultitopicRead)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.kafkaMultitopicWrite)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.hdfs)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.hbase)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.ERROR.multiETL)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.CHECKPOINT.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Structured.avroEncoder)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.kafka)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.solr)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.elastic)
    waspDB.upsert[PipegraphModel](TestPipegraphs.AVRO.Legacy.hdfs)

    waspDB.upsert[PipegraphModel](TestPipegraphs.Plaintext.Structured.kafkaMultitopicWrite)

    waspDB.upsert[PipegraphModel](TestPipegraphs.Binary.Structured.kafkaMultitopicWrite)
    waspDB.upsert[PipegraphModel](TestPipegraphs.SparkSessionErrors.pipegraph)


    /* BatchJobs */
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromElastic.toHdfsNested)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromSolr.toHdfsFlat)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromHdfs.flatToConsole)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromHdfs.toKafka)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromHdfs.nestedToConsole)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromHdfs.nestedToMongo)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromJdbc.mySqlToConsole)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.WithPostHook.nestedToConsole)
    waspDB.upsert[BatchJobModel](TestBatchJobModels.FromMongo.nestedToConsole)
    waspDB.upsert[BatchJobModel](TestGdprBatchJobModels.model)


    /* Test SchemaAvroManager */
    waspDB.upsert[TopicModel](TestSchemaAvroManager.topicAvro_v1)
    waspDB.upsert[TopicModel](TestSchemaAvroManager.topicAvro_v2)
    waspDB.upsert[TopicModel](TestSchemaAvroManager.topicAvro_v3)
    waspDB.upsert[ProducerModel](TestSchemaAvroManager.producer_v1)
    waspDB.upsert[ProducerModel](TestSchemaAvroManager.producer_v2)
    waspDB.upsert[KeyValueModel](TestSchemaAvroManagerKeyValueModel.avroSchemaManagerHBaseModel)
    waspDB.upsert[PipegraphModel](TestSchemaAvroManager.pipegraph)


    /* Test backlog */
    waspDB.upsert[TopicModel](TestTopicModel.monitoring)
    waspDB.upsert[ProducerModel](TestProducerModel.backlog)
    waspDB.upsert[ProducerModel](TestProducerModel.throughput)
  }
}
