package it.agilelab.bigdata.wasp.whitelabel.models.test

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.models.{LegacyStreamingETLModel, PipegraphModel, ReaderModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}

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
              Some(
                """
                  | nifi.process-group-id = "c116c98bd5c9"
                  | nifi.error-port = "e8dc48d4-633e-30f8-a59a-6f30f26c8917"
                  | nifi.variables = {}""".stripMargin)
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