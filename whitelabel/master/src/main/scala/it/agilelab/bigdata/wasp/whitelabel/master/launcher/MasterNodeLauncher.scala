package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import com.sksamuel.avro4s.AvroSchema
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import it.agilelab.bigdata.wasp.whitelabel.models.example._
import it.agilelab.bigdata.wasp.whitelabel.models.test._
import it.agilelab.darwin.manager.AvroSchemaManagerFactory
import org.apache.avro.Schema
import org.apache.commons.cli.CommandLine
import org.mongodb.scala.bson.BsonDocument

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
    val schemas: Seq[Schema] = Seq(
      AvroSchema[FakeData],
      AvroSchema[TopicAvro_v1],
      AvroSchema[TopicAvro_v2],
      new Schema.Parser().parse(TestTopicModel.avro.schema.toJson)
    )
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

    /* ProcessGroups */

    waspDB.upsert[ProcessGroupModel](ExampleProcessGroupModel.processGroup)

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
    waspDB.upsert[RawModel](TestRawModel.flat)   // used by TestBatchJobModels.FromHdfs.toConsole
    waspDB.upsert[RawModel](TestRawModel.text)   // used by TestBatchJobModels.FromHdfs.toConsole
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
    waspDB.upsert[ProducerModel](FakeDataProducerModel.fakeDataProducerSimulator) //EVENT ENGINE
    waspDB.upsert[TopicModel](FakeDataTopicModel.fakeDataTopicModel)              //EVENT ENGINE

    /* Pipegraphs */
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.console)
    waspDB.upsert[PipegraphModel](TestPipegraphs.JSON.Structured.nifi)
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

private[wasp] object ExampleProcessGroupModel {
  lazy val processGroup = ProcessGroupModel(
    "c116c98bd5c9",
    BsonDocument("""{
        |      "comments": "",
        |      "componentType": "PROCESS_GROUP",
        |      "connections": [
        |        {
        |          "backPressureDataSizeThreshold": "1 GB",
        |          "backPressureObjectThreshold": 10000,
        |          "bends": [],
        |          "componentType": "CONNECTION",
        |          "destination": {
        |            "groupId": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |            "id": "e8dc48d4-633e-30f8-a59a-6f30f26c8917",
        |            "name": "wasp-error",
        |            "type": "OUTPUT_PORT"
        |          },
        |          "flowFileExpiration": "0 sec",
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "2070f0a0-46f5-3a76-bdfd-bd5b9ce7330e",
        |          "labelIndex": 1,
        |          "loadBalanceCompression": "DO_NOT_COMPRESS",
        |          "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        |          "name": "",
        |          "partitioningAttribute": "",
        |          "prioritizers": [],
        |          "selectedRelationships": [
        |            "failure"
        |          ],
        |          "source": {
        |            "comments": "",
        |            "groupId": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |            "id": "35173b75-c2fe-3e65-b16c-c1e377a23ae3",
        |            "name": "UpdateRecord",
        |            "type": "PROCESSOR"
        |          },
        |          "zIndex": 0
        |        },
        |        {
        |          "backPressureDataSizeThreshold": "1 GB",
        |          "backPressureObjectThreshold": 10000,
        |          "bends": [],
        |          "componentType": "CONNECTION",
        |          "destination": {
        |            "groupId": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |            "id": "84cd6bce-4cf1-3ba8-ba04-400a54ff454f",
        |            "name": "wasp-output",
        |            "type": "OUTPUT_PORT"
        |          },
        |          "flowFileExpiration": "0 sec",
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "c3077697-b457-347f-87d8-9b7453e7b9e0",
        |          "labelIndex": 1,
        |          "loadBalanceCompression": "DO_NOT_COMPRESS",
        |          "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        |          "name": "",
        |          "partitioningAttribute": "",
        |          "prioritizers": [],
        |          "selectedRelationships": [
        |            "success"
        |          ],
        |          "source": {
        |            "comments": "",
        |            "groupId": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |            "id": "35173b75-c2fe-3e65-b16c-c1e377a23ae3",
        |            "name": "UpdateRecord",
        |            "type": "PROCESSOR"
        |          },
        |          "zIndex": 0
        |        },
        |        {
        |          "backPressureDataSizeThreshold": "1 GB",
        |          "backPressureObjectThreshold": 10000,
        |          "bends": [],
        |          "componentType": "CONNECTION",
        |          "destination": {
        |            "comments": "",
        |            "groupId": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |            "id": "35173b75-c2fe-3e65-b16c-c1e377a23ae3",
        |            "name": "UpdateRecord",
        |            "type": "PROCESSOR"
        |          },
        |          "flowFileExpiration": "0 sec",
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "14d262ec-a201-35ce-ae4c-6d5108f7a922",
        |          "labelIndex": 1,
        |          "loadBalanceCompression": "DO_NOT_COMPRESS",
        |          "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        |          "name": "",
        |          "partitioningAttribute": "",
        |          "prioritizers": [],
        |          "selectedRelationships": [
        |            ""
        |          ],
        |          "source": {
        |            "groupId": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |            "id": "bcb83abe-740a-316a-ae7b-40c6e6773673",
        |            "name": "wasp-input",
        |            "type": "INPUT_PORT"
        |          },
        |          "zIndex": 0
        |        }
        |      ],
        |      "controllerServices": [
        |        {
        |          "bundle": {
        |            "artifact": "nifi-record-serialization-services-nar",
        |            "group": "org.apache.nifi",
        |            "version": "1.11.4"
        |          },
        |          "componentType": "CONTROLLER_SERVICE",
        |          "controllerServiceApis": [
        |            {
        |              "bundle": {
        |                "artifact": "nifi-standard-services-api-nar",
        |                "group": "org.apache.nifi",
        |                "version": "1.11.4"
        |              },
        |              "type": "org.apache.nifi.serialization.RecordReaderFactory"
        |            }
        |          ],
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "72fa712e-27fe-3f57-968e-aea1ddec970c",
        |          "name": "JsonTreeReader",
        |          "properties": {
        |            "schema-access-strategy": "infer-schema",
        |            "schema-name": "${schema.name}",
        |            "schema-text": "${avro.schema}"
        |          },
        |          "propertyDescriptors": {
        |            "Date Format": {
        |              "displayName": "Date Format",
        |              "identifiesControllerService": false,
        |              "name": "Date Format",
        |              "sensitive": false
        |            },
        |            "Time Format": {
        |              "displayName": "Time Format",
        |              "identifiesControllerService": false,
        |              "name": "Time Format",
        |              "sensitive": false
        |            },
        |            "Timestamp Format": {
        |              "displayName": "Timestamp Format",
        |              "identifiesControllerService": false,
        |              "name": "Timestamp Format",
        |              "sensitive": false
        |            },
        |            "schema-access-strategy": {
        |              "displayName": "Schema Access Strategy",
        |              "identifiesControllerService": false,
        |              "name": "schema-access-strategy",
        |              "sensitive": false
        |            },
        |            "schema-branch": {
        |              "displayName": "Schema Branch",
        |              "identifiesControllerService": false,
        |              "name": "schema-branch",
        |              "sensitive": false
        |            },
        |            "schema-inference-cache": {
        |              "displayName": "Schema Inference Cache",
        |              "identifiesControllerService": true,
        |              "name": "schema-inference-cache",
        |              "sensitive": false
        |            },
        |            "schema-name": {
        |              "displayName": "Schema Name",
        |              "identifiesControllerService": false,
        |              "name": "schema-name",
        |              "sensitive": false
        |            },
        |            "schema-registry": {
        |              "displayName": "Schema Registry",
        |              "identifiesControllerService": true,
        |              "name": "schema-registry",
        |              "sensitive": false
        |            },
        |            "schema-text": {
        |              "displayName": "Schema Text",
        |              "identifiesControllerService": false,
        |              "name": "schema-text",
        |              "sensitive": false
        |            },
        |            "schema-version": {
        |              "displayName": "Schema Version",
        |              "identifiesControllerService": false,
        |              "name": "schema-version",
        |              "sensitive": false
        |            }
        |          },
        |          "type": "org.apache.nifi.json.JsonTreeReader"
        |        },
        |        {
        |          "bundle": {
        |            "artifact": "nifi-record-serialization-services-nar",
        |            "group": "org.apache.nifi",
        |            "version": "1.11.4"
        |          },
        |          "componentType": "CONTROLLER_SERVICE",
        |          "controllerServiceApis": [
        |            {
        |              "bundle": {
        |                "artifact": "nifi-standard-services-api-nar",
        |                "group": "org.apache.nifi",
        |                "version": "1.11.4"
        |              },
        |              "type": "org.apache.nifi.serialization.RecordSetWriterFactory"
        |            }
        |          ],
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "372aa60b-f822-3e6c-a7e6-8f933294195a",
        |          "name": "JsonRecordSetWriter",
        |          "properties": {
        |            "Pretty Print JSON": "false",
        |            "Schema Write Strategy": "no-schema",
        |            "compression-format": "none",
        |            "compression-level": "1",
        |            "output-grouping": "output-array",
        |            "schema-access-strategy": "inherit-record-schema",
        |            "schema-name": "${schema.name}",
        |            "schema-text": "${avro.schema}",
        |            "suppress-nulls": "never-suppress"
        |          },
        |          "propertyDescriptors": {
        |            "Date Format": {
        |              "displayName": "Date Format",
        |              "identifiesControllerService": false,
        |              "name": "Date Format",
        |              "sensitive": false
        |            },
        |            "Pretty Print JSON": {
        |              "displayName": "Pretty Print JSON",
        |              "identifiesControllerService": false,
        |              "name": "Pretty Print JSON",
        |              "sensitive": false
        |            },
        |            "Schema Write Strategy": {
        |              "displayName": "Schema Write Strategy",
        |              "identifiesControllerService": false,
        |              "name": "Schema Write Strategy",
        |              "sensitive": false
        |            },
        |            "Time Format": {
        |              "displayName": "Time Format",
        |              "identifiesControllerService": false,
        |              "name": "Time Format",
        |              "sensitive": false
        |            },
        |            "Timestamp Format": {
        |              "displayName": "Timestamp Format",
        |              "identifiesControllerService": false,
        |              "name": "Timestamp Format",
        |              "sensitive": false
        |            },
        |            "compression-format": {
        |              "displayName": "Compression Format",
        |              "identifiesControllerService": false,
        |              "name": "compression-format",
        |              "sensitive": false
        |            },
        |            "compression-level": {
        |              "displayName": "Compression Level",
        |              "identifiesControllerService": false,
        |              "name": "compression-level",
        |              "sensitive": false
        |            },
        |            "output-grouping": {
        |              "displayName": "Output Grouping",
        |              "identifiesControllerService": false,
        |              "name": "output-grouping",
        |              "sensitive": false
        |            },
        |            "schema-access-strategy": {
        |              "displayName": "Schema Access Strategy",
        |              "identifiesControllerService": false,
        |              "name": "schema-access-strategy",
        |              "sensitive": false
        |            },
        |            "schema-branch": {
        |              "displayName": "Schema Branch",
        |              "identifiesControllerService": false,
        |              "name": "schema-branch",
        |              "sensitive": false
        |            },
        |            "schema-cache": {
        |              "displayName": "Schema Cache",
        |              "identifiesControllerService": true,
        |              "name": "schema-cache",
        |              "sensitive": false
        |            },
        |            "schema-name": {
        |              "displayName": "Schema Name",
        |              "identifiesControllerService": false,
        |              "name": "schema-name",
        |              "sensitive": false
        |            },
        |            "schema-registry": {
        |              "displayName": "Schema Registry",
        |              "identifiesControllerService": true,
        |              "name": "schema-registry",
        |              "sensitive": false
        |            },
        |            "schema-text": {
        |              "displayName": "Schema Text",
        |              "identifiesControllerService": false,
        |              "name": "schema-text",
        |              "sensitive": false
        |            },
        |            "schema-version": {
        |              "displayName": "Schema Version",
        |              "identifiesControllerService": false,
        |              "name": "schema-version",
        |              "sensitive": false
        |            },
        |            "suppress-nulls": {
        |              "displayName": "Suppress Null Values",
        |              "identifiesControllerService": false,
        |              "name": "suppress-nulls",
        |              "sensitive": false
        |            }
        |          },
        |          "type": "org.apache.nifi.json.JsonRecordSetWriter"
        |        }
        |      ],
        |      "funnels": [],
        |      "identifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |      "inputPorts": [
        |        {
        |          "allowRemoteAccess": false,
        |          "componentType": "INPUT_PORT",
        |          "concurrentlySchedulableTaskCount": 1,
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "bcb83abe-740a-316a-ae7b-40c6e6773673",
        |          "name": "wasp-input",
        |          "position": {
        |            "x": 1080,
        |            "y": 24
        |          },
        |          "type": "INPUT_PORT"
        |        }
        |      ],
        |      "labels": [],
        |      "name": "nuovo",
        |      "outputPorts": [
        |        {
        |          "allowRemoteAccess": false,
        |          "componentType": "OUTPUT_PORT",
        |          "concurrentlySchedulableTaskCount": 1,
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "e8dc48d4-633e-30f8-a59a-6f30f26c8917",
        |          "name": "wasp-error",
        |          "position": {
        |            "x": 1312,
        |            "y": 384
        |          },
        |          "type": "OUTPUT_PORT"
        |        },
        |        {
        |          "allowRemoteAccess": false,
        |          "componentType": "OUTPUT_PORT",
        |          "concurrentlySchedulableTaskCount": 1,
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "84cd6bce-4cf1-3ba8-ba04-400a54ff454f",
        |          "name": "wasp-output",
        |          "position": {
        |            "x": 1888,
        |            "y": 88
        |          },
        |          "type": "OUTPUT_PORT"
        |        }
        |      ],
        |      "position": {
        |        "x": 0,
        |        "y": 0
        |      },
        |      "processGroups": [],
        |      "processors": [
        |        {
        |          "autoTerminatedRelationships": [],
        |          "bulletinLevel": "WARN",
        |          "bundle": {
        |            "artifact": "nifi-standard-nar",
        |            "group": "org.apache.nifi",
        |            "version": "1.11.4"
        |          },
        |          "comments": "",
        |          "componentType": "PROCESSOR",
        |          "concurrentlySchedulableTaskCount": 1,
        |          "executionNode": "ALL",
        |          "groupIdentifier": "02f8383b-c58f-3b93-b924-0c0793e1eec7",
        |          "identifier": "35173b75-c2fe-3e65-b16c-c1e377a23ae3",
        |          "name": "UpdateRecord",
        |          "penaltyDuration": "30 sec",
        |          "position": {
        |            "x": 1064,
        |            "y": 152
        |          },
        |          "properties": {
        |            "/id": "${field.value} pippo",
        |            "record-reader": "72fa712e-27fe-3f57-968e-aea1ddec970c",
        |            "record-writer": "372aa60b-f822-3e6c-a7e6-8f933294195a",
        |            "replacement-value-strategy": "literal-value"
        |          },
        |          "propertyDescriptors": {
        |            "/id": {
        |              "displayName": "/id",
        |              "identifiesControllerService": false,
        |              "name": "/id",
        |              "sensitive": false
        |            },
        |            "record-reader": {
        |              "displayName": "Record Reader",
        |              "identifiesControllerService": true,
        |              "name": "record-reader",
        |              "sensitive": false
        |            },
        |            "record-writer": {
        |              "displayName": "Record Writer",
        |              "identifiesControllerService": true,
        |              "name": "record-writer",
        |              "sensitive": false
        |            },
        |            "replacement-value-strategy": {
        |              "displayName": "Replacement Value Strategy",
        |              "identifiesControllerService": false,
        |              "name": "replacement-value-strategy",
        |              "sensitive": false
        |            }
        |          },
        |          "runDurationMillis": 0,
        |          "scheduledState": "ENABLED",
        |          "schedulingPeriod": "0 sec",
        |          "schedulingStrategy": "TIMER_DRIVEN",
        |          "style": {},
        |          "type": "org.apache.nifi.processors.standard.UpdateRecord",
        |          "yieldDuration": "1 sec"
        |        }
        |      ],
        |      "remoteProcessGroups": [],
        |      "variables": {}
        |    }""".stripMargin),
    "e8dc48d4-633e-30f8-a59a-6f30f26c8917"
  )
}
