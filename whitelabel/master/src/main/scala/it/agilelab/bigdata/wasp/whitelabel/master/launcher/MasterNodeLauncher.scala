package it.agilelab.bigdata.wasp.whitelabel.master.launcher


import it.agilelab.bigdata.wasp.master.launcher.MasterNodeLauncherTrait
import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.whitelabel.models.example._
import it.agilelab.bigdata.wasp.whitelabel.models.example.iot._
import it.agilelab.bigdata.wasp.whitelabel.models.test._
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
  }

  private def addExamplePipegraphs(): Unit = {

    /* standalone applications should prefer waspDB.insertIfNotExist() instead of waspDB.upsert() */

    /*
     * =================================================================================================================
     * Examples
     * =================================================================================================================
     */

    /* Topic for Producers, Pipegraphs */
    ConfigBL.topicBL.upsert(ExampleTopicModel.topic)

    /* Pipegraphs */
    ConfigBL.pipegraphBL.upsert(ExamplePipegraphModel.pipegraph)
    ConfigBL.pipegraphBL.upsert(IoTIndustrialPlantPipegraphModel.pipegraph)

    /* ProcessGroups */

    ConfigBL.processGroupBL.insert(ExampleProcessGroupModel.processGroup)

    /*
     * =================================================================================================================
     * Tests
     * =================================================================================================================
     */

    /* Topic, Index, Raw, SqlSource for Producers, Pipegraphs, BatchJobs */
    ConfigBL.topicBL.upsert(TestTopicModel.json)
    ConfigBL.topicBL.upsert(TestTopicModel.json2)
    ConfigBL.topicBL.upsert(TestTopicModel.json3)
    ConfigBL.topicBL.upsert(TestTopicModel.json4)
    ConfigBL.topicBL.upsert(TestTopicModel.json5)
    ConfigBL.topicBL.upsert(TestTopicModel.json6)
    ConfigBL.topicBL.upsert(TestTopicModel.jsonMultitopicRead)
    ConfigBL.topicBL.upsert(TestTopicModel.jsonMultitopicWrite)
    ConfigBL.topicBL.upsert(TestTopicModel.json2ForKafkaHeaders)
    ConfigBL.topicBL.upsert(TestTopicModel.jsonWithMetadata)
    ConfigBL.topicBL.upsert(TestTopicModel.jsonCheckpoint)
    ConfigBL.topicBL.upsert(TestTopicModel.avro)
    ConfigBL.topicBL.upsert(TestTopicModel.avro2)
    ConfigBL.topicBL.upsert(TestTopicModel.avro3)
    ConfigBL.topicBL.upsert(TestTopicModel.avro4)
    ConfigBL.topicBL.upsert(TestTopicModel.avro5)
    ConfigBL.topicBL.upsert(TestTopicModel.avroMultitopicRead)
    ConfigBL.topicBL.upsert(TestTopicModel.avroMultitopicWrite)
    ConfigBL.topicBL.upsert(TestTopicModel.avro2ForKafkaHeaders)
    ConfigBL.topicBL.upsert(TestTopicModel.avroCheckpoint)
    ConfigBL.topicBL.upsert(TestTopicModel.plaintext1)
    ConfigBL.topicBL.upsert(TestTopicModel.plaintext2)
    ConfigBL.topicBL.upsert(TestTopicModel.plaintextMultitopic)
    ConfigBL.topicBL.upsert(TestTopicModel.binary1)
    ConfigBL.topicBL.upsert(TestTopicModel.binary2)
    ConfigBL.topicBL.upsert(TestTopicModel.binaryMultitopic)
    ConfigBL.topicBL.upsert(TestTopicModel.avro_key_schema)
    ConfigBL.topicBL.upsert(TestTopicModel.avro_key_schema2)
    ConfigBL.topicBL.upsert(TestTopicModel.multitopicreadjsonavro)
    ConfigBL.topicBL.upsert(TestTopicModel.multitopicreadjson)
    ConfigBL.indexBL.upsert(TestIndexModel.solr)
    ConfigBL.indexBL.upsert(TestIndexModel.elastic)
    ConfigBL.rawBL.upsert(TestRawModel.nested) // used by TestPipegraphs.JSON.XYZ.hdfs
    ConfigBL.rawBL.upsert(TestRawModel.flat) // used by TestBatchJobModels.FromHdfs.toConsole
    ConfigBL.rawBL.upsert(TestRawModel.text) // used by TestBatchJobModels.FromHdfs.toConsole
    ConfigBL.keyValueBL.upsert(TestKeyValueModel.hbase)
    ConfigBL.keyValueBL.upsert(TestKeyValueModel.hbaseMultipleClusteringKeyValueModel)
    ConfigBL.sqlSourceBl.upsert(TestSqlSouceModel.mySql)
    ConfigBL.documentBL.upsert(TestMongoModel.writeToMongo)
    ConfigBL.rawBL.upsert(TestGdprBatchJobModels.outputRawModel)
    ConfigBL.rawBL.upsert(TestGdprBatchJobModels.inputRawModel)
    ConfigBL.rawBL.upsert(TestGdprBatchJobModels.dataRawModel)
    ConfigBL.topicBL.upsert(FakeDataTopicModel.fakeDataTopicModel) //EVENT ENGINE
    ConfigBL.topicBL.upsert(IoTIndustrialPlantTopicModel.industrialPlantTopicModel) //IoT
    ConfigBL.indexBL.upsert(IoTIndustrialPlantIndexModel()) //IoT
    ConfigBL.topicBL.upsert(TestTopicModel.dbzMutations)
    ConfigBL.cdcBL.upsert(TestCdcModel.debeziumMutation)
    ConfigBL.genericBL.upsert(TestGenericModel.genericJson)


    /* Producers */
    ConfigBL.producerBL.upsert(TestProducerModel.json)
    ConfigBL.producerBL.upsert(TestProducerModel.jsonWithMetadata)
    ConfigBL.producerBL.upsert(TestProducerModel.jsonCheckpoint)
    ConfigBL.producerBL.upsert(TestProducerModel.avro)
    ConfigBL.producerBL.upsert(TestProducerModel.avro_key_schema)
    ConfigBL.producerBL.upsert(TestProducerModel.avroCheckpoint)
    ConfigBL.producerBL.upsert(TestProducerModel.jsonHbaseMultipleClustering)
    ConfigBL.producerBL.upsert(FakeDataProducerModel.fakeDataProducerSimulator) //EVENT ENGINE
    ConfigBL.producerBL.upsert(IoTIndustrialPlantProducerModel.iotIndustrialPlantProducer) //IoT


    /* Free code models */

    ConfigBL.freeCodeBL.insert(TestFreeCodeModels.testFreeCode)

    /* Pipegraphs */

    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.console)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.freecode)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.nifi)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.mongo)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.consoleWithMetadata)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.kafka)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.kafkaHeaders)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.kafkaMultitopicRead)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.kafkaMultitopicWrite)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.solr)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.elastic)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.hdfs)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.hbase)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.hbaseMultipleClustering)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.multiETL)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.httpPost)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.httpPostHeaders)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.httpEnrichment)

    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.generic)

//    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.httpsPost)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.ERROR.multiETL)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.CHECKPOINT.console)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Legacy.console)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Legacy.kafka)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Legacy.solr)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Legacy.elastic)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Legacy.hdfs)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.JSON.Structured.autoDataLakeDebeziumMutations)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.console)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.kafka)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.kafka_key_schema)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.kafkaHeaders)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.kafkaMultitopicRead)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.kafkaMultitopicWrite)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.solr)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.elastic)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.hdfs)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.hbase)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.multiETL)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.ERROR.multiETL)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.CHECKPOINT.console)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Structured.avroEncoder)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Legacy.console)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Legacy.kafka)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Legacy.solr)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Legacy.elastic)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.AVRO.Legacy.hdfs)

    ConfigBL.pipegraphBL.upsert(TestPipegraphs.Plaintext.Structured.kafkaMultitopicWrite)

    ConfigBL.pipegraphBL.upsert(TestPipegraphs.Binary.Structured.kafkaMultitopicWrite)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.SparkSessionErrors.pipegraph)

    ConfigBL.pipegraphBL.upsert(TestPipegraphs.MultiTopicReader.Structured.kafkaMultitopicReadDifferent)
    ConfigBL.pipegraphBL.upsert(TestPipegraphs.MultiTopicReader.Structured.kafkaMultitopicReadSame)

    /* BatchJobs */
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromElastic.toHdfsNested)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromSolr.toHdfsFlat)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromHdfs.flatToConsole)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromHdfs.toKafka)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromHdfs.nestedToConsole)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromHdfs.nestedToMongo)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromJdbc.mySqlToConsole)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.WithPostHook.nestedToConsole)
    ConfigBL.batchJobBL.upsert(TestBatchJobModels.FromMongo.nestedToConsole)
    ConfigBL.batchJobBL.upsert(TestGdprBatchJobModels.model)

    /* Test SchemaAvroManager */
    ConfigBL.topicBL.upsert(TestSchemaAvroManager.topicAvro_v1)
    ConfigBL.topicBL.upsert(TestSchemaAvroManager.topicAvro_v2)
    ConfigBL.topicBL.upsert(TestSchemaAvroManager.topicAvro_v3)
    ConfigBL.producerBL.upsert(TestSchemaAvroManager.producer_v1)
    ConfigBL.producerBL.upsert(TestSchemaAvroManager.producer_v2)
    ConfigBL.keyValueBL.upsert(TestSchemaAvroManagerKeyValueModel.avroSchemaManagerHBaseModel)
    ConfigBL.pipegraphBL.upsert(TestSchemaAvroManager.pipegraph)

    /* Test backlog */
    ConfigBL.topicBL.upsert(TestTopicModel.monitoring)
    ConfigBL.producerBL.upsert(TestProducerModel.backlog)
    ConfigBL.producerBL.upsert(TestProducerModel.throughput)

    /* Test Http model*/
    ConfigBL.httpBl.upsert(TestHttpModel.httpPost)
    ConfigBL.httpBl.upsert(TestHttpModel.httpsPost)
    ConfigBL.httpBl.upsert(TestHttpModel.httpPostHeaders)
  }
}

private[wasp] object ExampleProcessGroupModel {
  lazy val processGroup = ProcessGroupModel(
    "c116c98bd5c9",
    BsonDocument(
      """{
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
