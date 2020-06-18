package it.agilelab.bigdata.wasp.whitelabel.models.test

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.models._

private[wasp] object TestPipegraphs {

  object JSON {

    object Structured {

      lazy val console = PipegraphModel(
        name = "TestConsoleWriterStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy= None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val nifi = PipegraphModel(
        name = "TestConsoleWriterNifiStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterNifiStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = Some(StrategyModel(
             "it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiStrategy",
             Some("""
                | nifi.flow = "{\r\n        \"comments\" : \"\",\r\n        \"componentType\" : \"PROCESS_GROUP\",\r\n        \"connections\" : [ {\r\n          \"backPressureDataSizeThreshold\" : \"1 GB\",\r\n          \"backPressureObjectThreshold\" : 10000,\r\n          \"bends\" : [ ],\r\n          \"componentType\" : \"CONNECTION\",\r\n          \"destination\" : {\r\n            \"comments\" : \"\",\r\n            \"groupId\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n            \"id\" : \"584b7ff6-190d-32d3-bf86-616e0490cd82\",\r\n            \"name\" : \"UpdateRecord\",\r\n            \"type\" : \"PROCESSOR\"\r\n          },\r\n          \"flowFileExpiration\" : \"0 sec\",\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"a31d7bd5-6599-3930-a4f6-17986d9ff0fe\",\r\n          \"labelIndex\" : 1,\r\n          \"loadBalanceCompression\" : \"DO_NOT_COMPRESS\",\r\n          \"loadBalanceStrategy\" : \"DO_NOT_LOAD_BALANCE\",\r\n          \"name\" : \"\",\r\n          \"partitioningAttribute\" : \"\",\r\n          \"prioritizers\" : [ ],\r\n          \"selectedRelationships\" : [ \"\" ],\r\n          \"source\" : {\r\n            \"groupId\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n            \"id\" : \"ef515726-8635-396f-af63-ac440f463ed4\",\r\n            \"name\" : \"wasp-input\",\r\n            \"type\" : \"INPUT_PORT\"\r\n          },\r\n          \"zIndex\" : 0\r\n        }, {\r\n          \"backPressureDataSizeThreshold\" : \"1 GB\",\r\n          \"backPressureObjectThreshold\" : 10000,\r\n          \"bends\" : [ ],\r\n          \"componentType\" : \"CONNECTION\",\r\n          \"destination\" : {\r\n            \"groupId\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n            \"id\" : \"831e3a07-f6b1-32e9-aac9-c116c98bd5c9\",\r\n            \"name\" : \"wasp-error\",\r\n            \"type\" : \"OUTPUT_PORT\"\r\n          },\r\n          \"flowFileExpiration\" : \"0 sec\",\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"df9af29a-bbdf-3afb-a41f-48668b3c0b92\",\r\n          \"labelIndex\" : 1,\r\n          \"loadBalanceCompression\" : \"DO_NOT_COMPRESS\",\r\n          \"loadBalanceStrategy\" : \"DO_NOT_LOAD_BALANCE\",\r\n          \"name\" : \"\",\r\n          \"partitioningAttribute\" : \"\",\r\n          \"prioritizers\" : [ ],\r\n          \"selectedRelationships\" : [ \"failure\" ],\r\n          \"source\" : {\r\n            \"comments\" : \"\",\r\n            \"groupId\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n            \"id\" : \"584b7ff6-190d-32d3-bf86-616e0490cd82\",\r\n            \"name\" : \"UpdateRecord\",\r\n            \"type\" : \"PROCESSOR\"\r\n          },\r\n          \"zIndex\" : 0\r\n        }, {\r\n          \"backPressureDataSizeThreshold\" : \"1 GB\",\r\n          \"backPressureObjectThreshold\" : 10000,\r\n          \"bends\" : [ ],\r\n          \"componentType\" : \"CONNECTION\",\r\n          \"destination\" : {\r\n            \"groupId\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n            \"id\" : \"ce78911c-239d-31c4-a1af-007fe62aec78\",\r\n            \"name\" : \"wasp-output\",\r\n            \"type\" : \"OUTPUT_PORT\"\r\n          },\r\n          \"flowFileExpiration\" : \"0 sec\",\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"bc03f5b0-f526-313b-978f-317c58dd6557\",\r\n          \"labelIndex\" : 1,\r\n          \"loadBalanceCompression\" : \"DO_NOT_COMPRESS\",\r\n          \"loadBalanceStrategy\" : \"DO_NOT_LOAD_BALANCE\",\r\n          \"name\" : \"\",\r\n          \"partitioningAttribute\" : \"\",\r\n          \"prioritizers\" : [ ],\r\n          \"selectedRelationships\" : [ \"success\" ],\r\n          \"source\" : {\r\n            \"comments\" : \"\",\r\n            \"groupId\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n            \"id\" : \"584b7ff6-190d-32d3-bf86-616e0490cd82\",\r\n            \"name\" : \"UpdateRecord\",\r\n            \"type\" : \"PROCESSOR\"\r\n          },\r\n          \"zIndex\" : 0\r\n        } ],\r\n        \"controllerServices\" : [ {\r\n          \"bundle\" : {\r\n            \"artifact\" : \"nifi-record-serialization-services-nar\",\r\n            \"group\" : \"org.apache.nifi\",\r\n            \"version\" : \"1.11.4\"\r\n          },\r\n          \"componentType\" : \"CONTROLLER_SERVICE\",\r\n          \"controllerServiceApis\" : [ {\r\n            \"bundle\" : {\r\n              \"artifact\" : \"nifi-standard-services-api-nar\",\r\n              \"group\" : \"org.apache.nifi\",\r\n              \"version\" : \"1.11.4\"\r\n            },\r\n            \"type\" : \"org.apache.nifi.serialization.RecordReaderFactory\"\r\n          } ],\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"ac1c5f83-f793-38e1-a9a8-f4a6cbcf5052\",\r\n          \"name\" : \"JsonTreeReader\",\r\n          \"properties\" : {\r\n            \"schema-name\" : \"${schema.name}\",\r\n            \"schema-access-strategy\" : \"infer-schema\",\r\n            \"schema-text\" : \"${avro.schema}\"\r\n          },\r\n          \"propertyDescriptors\" : {\r\n            \"Timestamp Format\" : {\r\n              \"displayName\" : \"Timestamp Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Timestamp Format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-inference-cache\" : {\r\n              \"displayName\" : \"Schema Inference Cache\",\r\n              \"identifiesControllerService\" : true,\r\n              \"name\" : \"schema-inference-cache\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Date Format\" : {\r\n              \"displayName\" : \"Date Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Date Format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-branch\" : {\r\n              \"displayName\" : \"Schema Branch\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-branch\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-name\" : {\r\n              \"displayName\" : \"Schema Name\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-name\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-registry\" : {\r\n              \"displayName\" : \"Schema Registry\",\r\n              \"identifiesControllerService\" : true,\r\n              \"name\" : \"schema-registry\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Time Format\" : {\r\n              \"displayName\" : \"Time Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Time Format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-access-strategy\" : {\r\n              \"displayName\" : \"Schema Access Strategy\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-access-strategy\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-version\" : {\r\n              \"displayName\" : \"Schema Version\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-version\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-text\" : {\r\n              \"displayName\" : \"Schema Text\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-text\",\r\n              \"sensitive\" : false\r\n            }\r\n          },\r\n          \"type\" : \"org.apache.nifi.json.JsonTreeReader\"\r\n        }, {\r\n          \"bundle\" : {\r\n            \"artifact\" : \"nifi-record-serialization-services-nar\",\r\n            \"group\" : \"org.apache.nifi\",\r\n            \"version\" : \"1.11.4\"\r\n          },\r\n          \"componentType\" : \"CONTROLLER_SERVICE\",\r\n          \"controllerServiceApis\" : [ {\r\n            \"bundle\" : {\r\n              \"artifact\" : \"nifi-standard-services-api-nar\",\r\n              \"group\" : \"org.apache.nifi\",\r\n              \"version\" : \"1.11.4\"\r\n            },\r\n            \"type\" : \"org.apache.nifi.serialization.RecordSetWriterFactory\"\r\n          } ],\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"e1916067-e9b1-34af-a8fa-cce2162fc3e4\",\r\n          \"name\" : \"JsonRecordSetWriter\",\r\n          \"properties\" : {\r\n            \"compression-level\" : \"1\",\r\n            \"Pretty Print JSON\" : \"false\",\r\n            \"compression-format\" : \"none\",\r\n            \"Schema Write Strategy\" : \"no-schema\",\r\n            \"suppress-nulls\" : \"never-suppress\",\r\n            \"output-grouping\" : \"output-array\",\r\n            \"schema-name\" : \"${schema.name}\",\r\n            \"schema-access-strategy\" : \"inherit-record-schema\",\r\n            \"schema-text\" : \"${avro.schema}\"\r\n          },\r\n          \"propertyDescriptors\" : {\r\n            \"schema-branch\" : {\r\n              \"displayName\" : \"Schema Branch\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-branch\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"compression-level\" : {\r\n              \"displayName\" : \"Compression Level\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"compression-level\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-cache\" : {\r\n              \"displayName\" : \"Schema Cache\",\r\n              \"identifiesControllerService\" : true,\r\n              \"name\" : \"schema-cache\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Timestamp Format\" : {\r\n              \"displayName\" : \"Timestamp Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Timestamp Format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Date Format\" : {\r\n              \"displayName\" : \"Date Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Date Format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Pretty Print JSON\" : {\r\n              \"displayName\" : \"Pretty Print JSON\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Pretty Print JSON\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"compression-format\" : {\r\n              \"displayName\" : \"Compression Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"compression-format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Schema Write Strategy\" : {\r\n              \"displayName\" : \"Schema Write Strategy\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Schema Write Strategy\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"suppress-nulls\" : {\r\n              \"displayName\" : \"Suppress Null Values\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"suppress-nulls\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"output-grouping\" : {\r\n              \"displayName\" : \"Output Grouping\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"output-grouping\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-name\" : {\r\n              \"displayName\" : \"Schema Name\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-name\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-registry\" : {\r\n              \"displayName\" : \"Schema Registry\",\r\n              \"identifiesControllerService\" : true,\r\n              \"name\" : \"schema-registry\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"Time Format\" : {\r\n              \"displayName\" : \"Time Format\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"Time Format\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-access-strategy\" : {\r\n              \"displayName\" : \"Schema Access Strategy\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-access-strategy\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-version\" : {\r\n              \"displayName\" : \"Schema Version\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-version\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"schema-text\" : {\r\n              \"displayName\" : \"Schema Text\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"schema-text\",\r\n              \"sensitive\" : false\r\n            }\r\n          },\r\n          \"type\" : \"org.apache.nifi.json.JsonRecordSetWriter\"\r\n        } ],\r\n        \"funnels\" : [ ],\r\n        \"identifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n        \"inputPorts\" : [ {\r\n          \"allowRemoteAccess\" : false,\r\n          \"componentType\" : \"INPUT_PORT\",\r\n          \"concurrentlySchedulableTaskCount\" : 1,\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"ef515726-8635-396f-af63-ac440f463ed4\",\r\n          \"name\" : \"wasp-input\",\r\n          \"position\" : {\r\n            \"x\" : 304.0,\r\n            \"y\" : 304.0\r\n          },\r\n          \"type\" : \"INPUT_PORT\"\r\n        } ],\r\n        \"labels\" : [ ],\r\n        \"name\" : \"prova\",\r\n        \"outputPorts\" : [ {\r\n          \"allowRemoteAccess\" : false,\r\n          \"componentType\" : \"OUTPUT_PORT\",\r\n          \"concurrentlySchedulableTaskCount\" : 1,\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"ce78911c-239d-31c4-a1af-007fe62aec78\",\r\n          \"name\" : \"wasp-output\",\r\n          \"position\" : {\r\n            \"x\" : 1200.0,\r\n            \"y\" : 296.0\r\n          },\r\n          \"type\" : \"OUTPUT_PORT\"\r\n        }, {\r\n          \"allowRemoteAccess\" : false,\r\n          \"componentType\" : \"OUTPUT_PORT\",\r\n          \"concurrentlySchedulableTaskCount\" : 1,\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"831e3a07-f6b1-32e9-aac9-c116c98bd5c9\",\r\n          \"name\" : \"wasp-error\",\r\n          \"position\" : {\r\n            \"x\" : 1376.0,\r\n            \"y\" : 560.0\r\n          },\r\n          \"type\" : \"OUTPUT_PORT\"\r\n        } ],\r\n        \"position\" : {\r\n          \"x\" : 627.0,\r\n          \"y\" : 316.0\r\n        },\r\n        \"processGroups\" : [ ],\r\n        \"processors\" : [ {\r\n          \"autoTerminatedRelationships\" : [ ],\r\n          \"bulletinLevel\" : \"WARN\",\r\n          \"bundle\" : {\r\n            \"artifact\" : \"nifi-standard-nar\",\r\n            \"group\" : \"org.apache.nifi\",\r\n            \"version\" : \"1.11.4\"\r\n          },\r\n          \"comments\" : \"\",\r\n          \"componentType\" : \"PROCESSOR\",\r\n          \"concurrentlySchedulableTaskCount\" : 1,\r\n          \"executionNode\" : \"ALL\",\r\n          \"groupIdentifier\" : \"4d9f8cf3-a2ef-34a7-a766-383041bdda21\",\r\n          \"identifier\" : \"584b7ff6-190d-32d3-bf86-616e0490cd82\",\r\n          \"name\" : \"UpdateRecord\",\r\n          \"penaltyDuration\" : \"30 sec\",\r\n          \"position\" : {\r\n            \"x\" : 680.0,\r\n            \"y\" : 472.0\r\n          },\r\n          \"properties\" : {\r\n            \"record-writer\" : \"e1916067-e9b1-34af-a8fa-cce2162fc3e4\",\r\n            \"record-reader\" : \"ac1c5f83-f793-38e1-a9a8-f4a6cbcf5052\",\r\n            \"\/id\" : \"${field.value}ciccio\",\r\n            \"replacement-value-strategy\" : \"literal-value\"\r\n          },\r\n          \"propertyDescriptors\" : {\r\n            \"record-writer\" : {\r\n              \"displayName\" : \"Record Writer\",\r\n              \"identifiesControllerService\" : true,\r\n              \"name\" : \"record-writer\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"record-reader\" : {\r\n              \"displayName\" : \"Record Reader\",\r\n              \"identifiesControllerService\" : true,\r\n              \"name\" : \"record-reader\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"\/id\" : {\r\n              \"displayName\" : \"\/id\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"\/id\",\r\n              \"sensitive\" : false\r\n            },\r\n            \"replacement-value-strategy\" : {\r\n              \"displayName\" : \"Replacement Value Strategy\",\r\n              \"identifiesControllerService\" : false,\r\n              \"name\" : \"replacement-value-strategy\",\r\n              \"sensitive\" : false\r\n            }\r\n          },\r\n          \"runDurationMillis\" : 0,\r\n          \"scheduledState\" : \"ENABLED\",\r\n          \"schedulingPeriod\" : \"0 sec\",\r\n          \"schedulingStrategy\" : \"TIMER_DRIVEN\",\r\n          \"style\" : { },\r\n          \"type\" : \"org.apache.nifi.processors.standard.UpdateRecord\",\r\n          \"yieldDuration\" : \"1 sec\"\r\n        } ],\r\n        \"remoteProcessGroups\" : [ ],\r\n        \"variables\" : { }\r\n      }"
                | nifi.error-port = "831e3a07-f6b1-32e9-aac9-c116c98bd5c9"
                | nifi.variables = {}
                |
                |""".stripMargin)
            )),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val mongo = PipegraphModel(
        name = "TestMongoWriterStructuredJSONPipegraph",
        description = "Description of TestMongoWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.mongoDbWriter("Mongo Writer", TestMongoModel.writeToMongo, Map.empty),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val consoleWithMetadata = PipegraphModel(
        name = "TestConsoleWriterWithMetadataStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterWithMetadataStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "test-with-metadata-console-etl",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.jsonWithMetadata, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer with metadata"),
            mlModels = List(),
            strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestEchoStrategy")),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val kafka = PipegraphModel(
        name = "TestKafkaWriterStructuredJSONPipegraph",
        description = "Description of TestKafkaWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestKafkaWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.json2),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val kafkaHeaders = PipegraphModel(
        name = "TestKafkaWriterWithHeadersStructuredJSONPipegraph",
        description = "Test for reading/writing headers from/to Kafka for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "InsertKafkaHeaders",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.json2ForKafkaHeaders),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaHeaders),
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ShowKafkaMetadata",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json2, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      lazy val kafkaMultitopicRead = PipegraphModel(
        name = "TestKafkaReaderMultitopicStructuredJSONPipegraph",
        description = "Test for reading from multiple topics for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "CopyFromTest1ToTest2",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.json3),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ShowKafkaMetadata",
            streamingInput = StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.jsonMultitopicRead, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      lazy val kafkaMultitopicWrite = PipegraphModel(
        name = "TestKafkaWriterMultitopicStructuredJSONPipegraph",
        description = "Test for writing to multiple topics for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "MultitopicWrite",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaMultitopicWriter("Kafka Writer", TestTopicModel.jsonMultitopicWrite),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMultitopicWriteJson),
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ShowKafkaMetadata",
            streamingInput = StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.jsonMultitopicWrite, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      lazy val solr = PipegraphModel(
        name = "TestSolrWriterStructuredJSONPipegraph",
        description = "Description of TestSolrWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestSolrWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.solrWriter("Solr Writer", TestIndexModel.solr),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val elastic = PipegraphModel(
        name = "TestElasticWriterStructuredJSONPipegraph",
        description = "Description of TestElasticWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestElasticWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.elasticWriter("Elastic Writer", TestIndexModel.elastic),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hdfs = PipegraphModel(
        name = "TestHdfsWriterStructuredJSONPipegraph",
        description = "Description of TestHdfsWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHdfsWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.rawWriter("Raw Writer", TestRawModel.nested),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hbase = PipegraphModel(
        name = "TestHBaseWriterStructuredJSONPipegraph",
        description = "Description of TestHBaseWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHBaseWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.hbaseWriter("HBase Writer", TestKeyValueModel.hbase),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hbaseMultipleClustering = PipegraphModel(
        name = "TestHBaseMultiClusteringWriterStructuredJSONPipegraph",
        description = "Description of TestHBaseMultiClusteringWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHBaseMultiClusteringWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json6, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.hbaseWriter("HBase Writer", TestKeyValueModel.hbaseMultipleClusteringKeyValueModel),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val multiETL = PipegraphModel(
        name = "TestMultiEtlJSONPipegraph",
        description = "Description of TestMultiEtlJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents =
          console.structuredStreamingComponents :::
            solr.structuredStreamingComponents :::
            elastic.structuredStreamingComponents :::
            hdfs.structuredStreamingComponents,
        rtComponents = List(),

        dashboard = None
      )

      object ERROR {

        lazy val multiETL = PipegraphModel(
          name = "TestErrorMultiEtlJSONPipegraph",
          description = "Description of TestErrorMultiEtlJSONPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,

          legacyStreamingComponents = List(),
          structuredStreamingComponents =
            console.structuredStreamingComponents :::
              solr.structuredStreamingComponents :::
              elastic.structuredStreamingComponents :::
              hdfs.structuredStreamingComponents.map(
                _.copy(strategy = Some(StrategyModel.create("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestErrorStrategy",
                  ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1"""))))),

          rtComponents = List(),

          dashboard = None)
      }

      object CHECKPOINT {
        lazy val console = PipegraphModel(
          name = "TestCheckpointConsoleWriterStructuredJSONPipegraph",
          description = "Description of TestCheckpointConsoleWriterStructuredJSONPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,

          legacyStreamingComponents = List(),
          structuredStreamingComponents = List(
            StructuredStreamingETLModel(
              name = "ETL TestCheckpointConsoleWriterStructuredJSONPipegraph",
              streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.jsonCheckpoint, None),
              staticInputs = List.empty,
              streamingOutput = WriterModel.consoleWriter("Console Writer"),
              mlModels = List(),
              strategy = Some(StrategyModel.create(
                "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV1",
                //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV2",
                //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV3",
                //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV4",
                ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1"""))),
              triggerIntervalMs = None,
              options = Map()
            )
          ),
          rtComponents = List(),

          dashboard = None
        )
      }

    }

    object Legacy {
      lazy val console = PipegraphModel(
        name = "TestConsoleWriterLegacyJSONPipegraph",
        description = "Description of TestConsoleWriterLegacyJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestConsoleWriterLegacyJSONPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json)
            ),
            output = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val kafka = PipegraphModel(
        name = "TestKafkaWriterLegacyJSONPipegraph",
        description = "Description of TestKafkaWriterLegacyJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestKafkaWriterLegacyJSONPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json)
            ),
            output = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.json2),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )

        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val solr = PipegraphModel(
        name = "TestSolrWriterLegacyJSONPipegraph",
        description = "Description of TestSolrWriterLegacyJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestSolrWriterLegacyJSONPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json)
            ),
            output = WriterModel.solrWriter("Solr Writer", TestIndexModel.solr),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val elastic = PipegraphModel(
        name = "TestElasticWriterLegacyJSONPipegraph",
        description = "Description of TestElasticWriterLegacyJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestElasticWriterLegacyJSONPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json)
            ),
            output = WriterModel.elasticWriter("Elastic Writer", TestIndexModel.elastic),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hdfs = PipegraphModel(
        name = "TestHdfsWriterLegacyJSONPipegraph",
        description = "Description of TestHdfsWriterLegacyJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestHdfsrWriterLegacyJSONPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json)
            ),
            output = WriterModel.rawWriter("Raw Writer", TestRawModel.nested),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )
    }

  }

  object AVRO {

    object Structured {

      lazy val console = PipegraphModel(
        name = "TestConsoleWriterStructuredAVROPipegraph",
        description = "Description of TestConsoleWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val kafka = PipegraphModel(
        name = "TestKafkaWriterStructuredAVROPipegraph",
        description = "Description of TestKafkaWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestKafkaWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.avro2),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val kafkaHeaders = PipegraphModel(
        name = "TestKafkaWriterWithHeadersStructuredAVROPipegraph",
        description = "Test for reading/writing headers from/to Kafkafor AVRO topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "InsertKafkaHeaders",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.avro2ForKafkaHeaders),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaHeaders),
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ShowKafkaMetadata",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro2, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      lazy val kafkaMultitopicRead = PipegraphModel(
        name = "TestKafkaReaderMultitopicStructuredAVROPipegraph",
        description = "Test for reading from multiple topics for AVRO topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "CopyFromTest1ToTest2",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.avro3),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ShowKafkaMetadata",
            streamingInput = StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.avroMultitopicRead, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      lazy val kafkaMultitopicWrite = PipegraphModel(
        name = "TestKafkaWriterMultitopicStructuredAVROPipegraph",
        description = "Test for writing to multiple topics for AVRO topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "MultitopicWrite",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaMultitopicWriter("Kafka Writer", TestTopicModel.avroMultitopicWrite),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMultitopicWriteAvro),
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ShowKafkaMetadata",
            streamingInput = StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.avroMultitopicWrite, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      lazy val solr = PipegraphModel(
        name = "TestSolrWriterStructuredAVROPipegraph",
        description = "Description of TestSolrWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestSolrWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.solrWriter("Solr Writer", TestIndexModel.solr),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val elastic = PipegraphModel(
        name = "TestElasticWriterStructuredAVROPipegraph",
        description = "Description of TestElasticWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestElasticWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.elasticWriter("Elastic Writer", TestIndexModel.elastic),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hdfs = PipegraphModel(
        name = "TestHdfsWriterStructuredAVROPipegraph",
        description = "Description of TestHdfsWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHdfsWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.rawWriter("Raw Writer", TestRawModel.nested),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hbase = PipegraphModel(
        name = "TestHBaseWriterStructuredAVROPipegraph",
        description = "Description of TestHBaseWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHBaseWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.hbaseWriter("HBase Writer", TestKeyValueModel.hbase),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None
      )

      lazy val multiETL = PipegraphModel(
        name = "TestMultiEtlAVROPipegraph",
        description = "Description of TestMultiEtlAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents =
          console.structuredStreamingComponents :::
            solr.structuredStreamingComponents :::
            elastic.structuredStreamingComponents :::
            hdfs.structuredStreamingComponents,
        rtComponents = List(),

        dashboard = None
      )

      lazy val avroEncoder = PipegraphModel(
        name = "TestAvroEncoderPipegraph",
        description = "Description of TestEncoderAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL AvroEncoderPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = Some(TestStrategies.testAvroEncoderStrategy),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

      object ERROR {

        lazy val multiETL = PipegraphModel(
          name = "TestErrorMultiEtlAVROPipegraph",
          description = "Description of TestErrorMultiEtlAVROPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,

          legacyStreamingComponents = List(),
          structuredStreamingComponents =
            console.structuredStreamingComponents :::
              solr.structuredStreamingComponents :::
              elastic.structuredStreamingComponents :::
              hdfs.structuredStreamingComponents.map(
                _.copy(strategy = Some(StrategyModel.create("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestErrorStrategy",
                  ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1"""))))),
          rtComponents = List(),

          dashboard = None
        )
      }

      object CHECKPOINT {
        lazy val console = PipegraphModel(
          name = "TestCheckpointConsoleWriterStructuredAVROPipegraph",
          description = "Description of TestCheckpointConsoleWriterStructuredAVROPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,

          legacyStreamingComponents = List(),
          structuredStreamingComponents = List(
            StructuredStreamingETLModel(
              name = "ETL TestCheckpointConsoleWriterStructuredAVROPipegraph",
              streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avroCheckpoint, None),
              staticInputs = List.empty,
              streamingOutput = WriterModel.consoleWriter("Console Writer"),
              mlModels = List(),
              strategy = Some(StrategyModel.create(
                "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV1",
                //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV2",
                //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV3",
                //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV4",
                ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1"""))),
              triggerIntervalMs = None,
              options = Map()
            )
          ),
          rtComponents = List(),

          dashboard = None
        )
      }

    }

    object Legacy {

      lazy val console = PipegraphModel(
        name = "TestConsoleWriterLegacyAVROPipegraph",
        description = "Description of TestConsoleWriterLegacyAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestConsoleWriterLegacyAVROPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro)
            ),
            output = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),
        dashboard = None
      )

      lazy val kafka = PipegraphModel(
        name = "TestKafkaWriterLegacyAVROPipegraph",
        description = "Description of TestKafkaWriterLegacyAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestKafkaWriterLegacyAVROPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro)
            ),
            output = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.avro2),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val solr = PipegraphModel(
        name = "TestSolrWriterLegacyAVROPipegraph",
        description = "Description of TestSolrWriterLegacyAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestSolrWriterLegacyAVROPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro)
            ),
            output = WriterModel.solrWriter("Solr Writer", TestIndexModel.solr),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val elastic = PipegraphModel(
        name = "TestElasticWriterLegacyAVROPipegraph",
        description = "Description of TestElasticWriterLegacyAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestElasticWriterLegacyAVROPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro)
            ),
            output = WriterModel.elasticWriter("Elastic Writer", TestIndexModel.elastic),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )

      lazy val hdfs = PipegraphModel(
        name = "TestHdfsWriterLegacyAVROPipegraph",
        description = "Description of TestHdfsWriterLegacyAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,

        legacyStreamingComponents = List(
          LegacyStreamingETLModel(
            name = "ETL TestHdfsrWriterLegacyAVROPipegraph",
            inputs = List(
              ReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro)
            ),
            output = WriterModel.rawWriter("Raw Writer", TestRawModel.nested),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None
      )
    }

  }

  object Plaintext {

    object Structured {

      lazy val kafkaMultitopicWrite = PipegraphModel(
        name = "TestKafkaPlaintextStructuredStreaming",
        description = "Test for reading/writing to multiple topics with key and headers for plaintext topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "GenerateAndWriteKafkaData",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaMultitopicWriter("Kafka Writer", TestTopicModel.plaintextMultitopic),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaPlaintext),
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ReadAndShowKafkaData",
            streamingInput = StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.plaintextMultitopic, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

    }

  }

  object Binary {

    object Structured {

      lazy val kafkaMultitopicWrite = PipegraphModel(
        name = "TestKafkaBinaryStructuredStreaming",
        description = "Test for reading/writing to multiple topics with key and headers for binary topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        legacyStreamingComponents = List(),
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "GenerateAndWriteKafkaData",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaMultitopicWriter("Kafka Writer", TestTopicModel.binaryMultitopic),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaBinary),
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ReadAndShowKafkaData",
            streamingInput = StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.binaryMultitopic, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        rtComponents = List(),
        dashboard = None
      )

    }

  }

  object ERROR {

    lazy val multiETL = PipegraphModel(
      name = "TestErrorMultiEtlPipegraph",
      description = "Description of TestErrorMultiEtlPipegraph",
      owner = "user",
      isSystem = false,
      creationTime = System.currentTimeMillis,

      legacyStreamingComponents = List(),
      structuredStreamingComponents =
        TestPipegraphs.AVRO.Structured.console.structuredStreamingComponents :::
          TestPipegraphs.AVRO.Structured.solr.structuredStreamingComponents :::
          TestPipegraphs.AVRO.Structured.elastic.structuredStreamingComponents :::
          TestPipegraphs.AVRO.Structured.hdfs.structuredStreamingComponents.map(
            _.copy(strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestErrorStrategy", None)))),
      rtComponents = List(),

      dashboard = None
    )
  }

  object SparkSessionErrors {

    lazy val pipegraph = PipegraphModel(
      name = "TestStrategiesSettingSparkSessionConf",
      description = "Two strategies setting different spark.sql.shuffle.partitions, should create independent values for each one",
      owner = "user",
      isSystem = false,
      creationTime = System.currentTimeMillis,

      legacyStreamingComponents = List(),
      structuredStreamingComponents = List(StructuredStreamingETLModel(
        name = "TestSetShufflePartitionsTo10ETL",
        streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
        staticInputs = List.empty,
        streamingOutput = WriterModel.consoleWriter("Console Writer"),
        mlModels = List(),
        strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestSetShufflePartitionsTo10Strategy")),
        triggerIntervalMs = None,
        options = Map()
      ),
        StructuredStreamingETLModel(
          name = "TestSetShufflePartitionsTo20ETL",
          streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
          staticInputs = List.empty,
          streamingOutput = WriterModel.consoleWriter("Console Writer"),
          mlModels = List(),
          strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestSetShufflePartitionsTo20Strategy")),
          triggerIntervalMs = None,
          options = Map()
        )),
      rtComponents = List(),

      dashboard = None
    )

  }

}