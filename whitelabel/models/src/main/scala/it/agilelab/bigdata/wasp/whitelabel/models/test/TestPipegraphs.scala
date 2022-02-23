package it.agilelab.bigdata.wasp.whitelabel.models.test

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.datastores.GenericProduct
import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.immutable.Map

private[wasp] object TestPipegraphs {

  object JSON {

    object Structured {

      lazy val autoDataLakeDebeziumMutations = PipegraphModel(
        name = "TestAutoDataLakeDebezium",
        description = "Description of TestAutoDataLakeDebezium",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestMutationsToSOEPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.dbzMutations, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.cdcWriter("Cdc Writer", TestCdcModel.debeziumMutation),
            mlModels = List(),
            strategy =
              Some(StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.DebeziumMutationStrategy")),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val console = PipegraphModel(
        name = "TestConsoleWriterStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val parallelWritePipegraph = PipegraphModel(
        name = "TestParallelWrite",
        description = "Description of TestParallelWrite",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestParallelWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.genericWriter("Parallel Writer", TestParallelWriteModel.parallelWriteModel),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      val genericContinuousProduct = GenericProduct("continuousUpdate", Some("continuous"))
      lazy val continuousUpdatePipegraph = PipegraphModel(
        name = "TestContinuousUpdate",
        description = "Description of TestContinuousUpdate",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestContinuousUpdateWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.genericWriter("Parallel Writer", TestParallelWriteModel.continuousUpdateModel),
            mlModels = List(),
            strategy = Some(TestStrategies.continuousUpdateStrategy),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val freecode = PipegraphModel(
        name = "TestConsoleWriterFreeCodeStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterFreeCodeStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = Some(
              StrategyModel(
                "it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy",
                Some("{ name = test-freecode }")
              )
            ),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None,
        labels = Set("free-code")
      )

      lazy val nifi = PipegraphModel(
        name = "TestConsoleWriterNifiStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestConsoleWriterNifiStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = Some(
              StrategyModel(
                "it.agilelab.bigdata.wasp.spark.plugins.nifi.NifiStrategy",
                Some("""
                  | nifi.process-group-id = "c116c98bd5c9"
                  | nifi.error-port = "e8dc48d4-633e-30f8-a59a-6f30f26c8917"
                  | nifi.variables = {}""".stripMargin)
              )
            ),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val mongo = PipegraphModel(
        name = "TestMongoWriterStructuredJSONPipegraph",
        description = "Description of TestMongoWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val consoleWithMetadata = PipegraphModel(
        name = "TestConsoleWriterWithMetadataStructuredJSONPipegraph",
        description = "Description of TestConsoleWriterWithMetadataStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "test-with-metadata-console-etl",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.jsonWithMetadata, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer with metadata"),
            mlModels = List(),
            strategy = Some(
              StrategyModel("it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestEchoStrategy")
            ),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val kafka = PipegraphModel(
        name = "TestKafkaWriterStructuredJSONPipegraph",
        description = "Description of TestKafkaWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val kafkaHeaders = PipegraphModel(
        name = "TestKafkaWriterWithHeadersStructuredJSONPipegraph",
        description = "Test for reading/writing headers from/to Kafka for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val kafkaMultitopicRead = PipegraphModel(
        name = "TestKafkaReaderMultitopicStructuredJSONPipegraph",
        description = "Test for reading from multiple topics for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.jsonMultitopicRead, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val kafkaMultitopicWrite = PipegraphModel(
        name = "TestKafkaWriterMultitopicStructuredJSONPipegraph",
        description = "Test for writing to multiple topics for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.jsonMultitopicWrite, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val kafkaMultitopicWriteMixed = PipegraphModel(
        name = "TestKafkaWriterMultitopicMixedStructuredPipegraph",
        description = "Test for writing to multiple topics with different schema and datatype",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "MultitopicWrite",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaMultitopicWriter("Kafka Writer", TestTopicModel.multitopicWriteMixed),
            mlModels = List(),
            strategy = Some(TestStrategies.multiTopicWriteMixedStrategy),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val solr = PipegraphModel(
        name = "TestSolrWriterStructuredJSONPipegraph",
        description = "Description of TestSolrWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val elastic = PipegraphModel(
        name = "TestElasticWriterStructuredJSONPipegraph",
        description = "Description of TestElasticWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val hdfs = PipegraphModel(
        name = "TestHdfsWriterStructuredJSONPipegraph",
        description = "Description of TestHdfsWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val hbase = PipegraphModel(
        name = "TestHBaseWriterStructuredJSONPipegraph",
        description = "Description of TestHBaseWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val hbaseMultipleClustering = PipegraphModel(
        name = "TestHBaseMultiClusteringWriterStructuredJSONPipegraph",
        description = "Description of TestHBaseMultiClusteringWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHBaseMultiClusteringWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json6, None),
            staticInputs = List.empty,
            streamingOutput =
              WriterModel.hbaseWriter("HBase Writer", TestKeyValueModel.hbaseMultipleClusteringKeyValueModel),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val multiETL = PipegraphModel(
        name = "TestMultiEtlJSONPipegraph",
        description = "Description of TestMultiEtlJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents =
          console.structuredStreamingComponents :::
            solr.structuredStreamingComponents :::
            elastic.structuredStreamingComponents :::
            hdfs.structuredStreamingComponents,
        dashboard = None
      )

      lazy val httpPost = PipegraphModel(
        name = "TestHttpPostWriterStructuredJSONPipegraph",
        description = "Description of TestHttpWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHttpPostWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.httpWriter("Http post writer", TestHttpModel.httpPost),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )
      lazy val httpPostHeaders = PipegraphModel(
        name = "TestHttpPostHeaderWriterStructuredJSONPipegraph",
        description = "Description of TestHttpWriterStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHttpPostHeaderWriterStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.httpWriter("Http post writer with headers", TestHttpModel.httpPostHeaders),
            mlModels = List(),
            strategy = Some(TestStrategies.testHttpHeaderStrategy),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )
//      lazy val httpsPost = PipegraphModel(
//        name = "TestHttpsPostWriterStructuredJSONPipegraph",
//        description = "Description of TestHttpsPostWriterStructuredJSONPipegraph",
//        owner = "user",
//        isSystem = false,
//        creationTime = System.currentTimeMillis,
//
//        structuredStreamingComponents = List(
//          StructuredStreamingETLModel(
//            name = "ETL TestHttpsPostWriterStructuredJSONPipegraph",
//            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
//            staticInputs = List.empty,
//            streamingOutput = WriterModel.httpWriter("Https post writer", TestHttpModel.httpsPost),
//            mlModels = List(),
//            strategy= None,
//            triggerIntervalMs = None,
//            options = Map()
//          )
//        ),
//        dashboard = None
//      )
//      lazy val httpsGet = PipegraphModel(
//        name = "TestHttpsGetWriterStructuredJSONPipegraph",
//        description = "Description of TestHttpsGetWriterStructuredJSONPipegraph",
//        owner = "user",
//        isSystem = false,
//        creationTime = System.currentTimeMillis,
//        structuredStreamingComponents = List(
//          StructuredStreamingETLModel(
//            name = "ETL TestHttpsGetWriterStructuredJSONPipegraph",
//            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
//            staticInputs = List.empty,
//            streamingOutput = WriterModel.httpWriter("Https get writer", TestHttpModel.httpsGet),
//            mlModels = List(),
//            strategy= None,
//            triggerIntervalMs = None,
//            options = Map()
//          )
//        ),
//
//
//        dashboard = None
//      )

      lazy val httpEnrichment = PipegraphModel(
        name = "TestHttpEnrichmentStructuredJSONPipegraph",
        description = "Description of TestHttpEnrichmentStructuredJSONPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestHttpEnrichmentStructuredJSONPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.json, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console Writer"),
            mlModels = List(),
            strategy = Some(TestStrategies.testHttpEnrichmentStrategy),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None,
        enrichmentSources = RestEnrichmentConfigModel(
          Map.apply(
            "getHttpExample" ->
              RestEnrichmentSource(
                "http",
                Map.apply(
                  "method" -> "GET",
                  "url"    -> "http://localhost:4480/${author}-v1/${version}/v2/${local}/123?id=test_id"
                ),
                Map.apply(
                  "Content-type" -> "text/plain",
                  "charset"      -> "ISO-8859-1"
                )
              ),
            "postHttpExample" ->
              RestEnrichmentSource(
                "http",
                Map.apply(
                  "method" -> "POST",
                  "url"    -> "http://localhost:4480/${author}-v1/${version}/v2/${local}/123?id=test_id"
                ),
                Map.apply(
                  "Content-type" -> "text/plain",
                  "charset"      -> "ISO-8859-1"
                )
              )
          )
        )
      )

      object ERROR {

        lazy val multiETL = PipegraphModel(
          name = "TestErrorMultiEtlJSONPipegraph",
          description = "Description of TestErrorMultiEtlJSONPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,
          structuredStreamingComponents =
            console.structuredStreamingComponents :::
              solr.structuredStreamingComponents :::
              elastic.structuredStreamingComponents :::
              hdfs.structuredStreamingComponents.map(
                _.copy(strategy = Some(
                  StrategyModel.create(
                    "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestErrorStrategy",
                    ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1""")
                  )
                )
                )
              ),
          dashboard = None
        )
      }

      object CHECKPOINT {
        lazy val console = PipegraphModel(
          name = "TestCheckpointConsoleWriterStructuredJSONPipegraph",
          description = "Description of TestCheckpointConsoleWriterStructuredJSONPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,
          structuredStreamingComponents = List(
            StructuredStreamingETLModel(
              name = "ETL TestCheckpointConsoleWriterStructuredJSONPipegraph",
              streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.jsonCheckpoint, None),
              staticInputs = List.empty,
              streamingOutput = WriterModel.consoleWriter("Console Writer"),
              mlModels = List(),
              strategy = Some(
                StrategyModel.create(
                  "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV1",
                  //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV2",
                  //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV3",
                  //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointJSONStrategyV4",
                  ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1""")
                )
              ),
              triggerIntervalMs = None,
              options = Map()
            )
          ),
          dashboard = None
        )
      }

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
        dashboard = None
      )

      lazy val kafka_key_schema = PipegraphModel(
        name = "TestKafkaWriterStructuredAVROKeySchemaPipegraph",
        description = "Description of TestKafkaWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "ETL TestKafkaWriterStructuredAVROPipegraph",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro_key_schema, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.kafkaWriter("Kafka Writer", TestTopicModel.avro_key_schema2),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ETL TestKafkaWriterStructuredAVROPipegraph Console",
            streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro_key_schema2, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Console", Map.empty),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val kafka = PipegraphModel(
        name = "TestKafkaWriterStructuredAVROPipegraph",
        description = "Description of TestKafkaWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val kafkaHeaders = PipegraphModel(
        name = "TestKafkaWriterWithHeadersStructuredAVROPipegraph",
        description = "Test for reading/writing headers from/to Kafkafor AVRO topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val kafkaMultitopicRead = PipegraphModel(
        name = "TestKafkaReaderMultitopicStructuredAVROPipegraph",
        description = "Test for reading from multiple topics for AVRO topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.avroMultitopicRead, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val kafkaMultitopicWrite = PipegraphModel(
        name = "TestKafkaWriterMultitopicStructuredAVROPipegraph",
        description = "Test for writing to multiple topics for AVRO topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.avroMultitopicWrite, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val solr = PipegraphModel(
        name = "TestSolrWriterStructuredAVROPipegraph",
        description = "Description of TestSolrWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val elastic = PipegraphModel(
        name = "TestElasticWriterStructuredAVROPipegraph",
        description = "Description of TestElasticWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val hdfs = PipegraphModel(
        name = "TestHdfsWriterStructuredAVROPipegraph",
        description = "Description of TestHdfsWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val hbase = PipegraphModel(
        name = "TestHBaseWriterStructuredAVROPipegraph",
        description = "Description of TestHBaseWriterStructuredAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      lazy val multiETL = PipegraphModel(
        name = "TestMultiEtlAVROPipegraph",
        description = "Description of TestMultiEtlAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents =
          console.structuredStreamingComponents :::
            solr.structuredStreamingComponents :::
            elastic.structuredStreamingComponents :::
            hdfs.structuredStreamingComponents,
        dashboard = None
      )

      lazy val avroEncoder = PipegraphModel(
        name = "TestAvroEncoderPipegraph",
        description = "Description of TestEncoderAVROPipegraph",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
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
        dashboard = None
      )

      object ERROR {

        lazy val multiETL = PipegraphModel(
          name = "TestErrorMultiEtlAVROPipegraph",
          description = "Description of TestErrorMultiEtlAVROPipegraph",
          owner = "user",
          isSystem = false,
          creationTime = System.currentTimeMillis,
          structuredStreamingComponents =
            console.structuredStreamingComponents :::
              solr.structuredStreamingComponents :::
              elastic.structuredStreamingComponents :::
              hdfs.structuredStreamingComponents.map(
                _.copy(strategy = Some(
                  StrategyModel.create(
                    "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestErrorStrategy",
                    ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1""")
                  )
                )
                )
              ),
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
          structuredStreamingComponents = List(
            StructuredStreamingETLModel(
              name = "ETL TestCheckpointConsoleWriterStructuredAVROPipegraph",
              streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avroCheckpoint, None),
              staticInputs = List.empty,
              streamingOutput = WriterModel.consoleWriter("Console Writer"),
              mlModels = List(),
              strategy = Some(
                StrategyModel.create(
                  "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV1",
                  //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV2",
                  //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV3",
                  //"it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestCheckpointAVROStrategyV4",
                  ConfigFactory.parseString("""stringKey = "stringValue", intKey = 1""")
                )
              ),
              triggerIntervalMs = None,
              options = Map()
            )
          ),
          dashboard = None
        )
      }

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
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.plaintextMultitopic, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
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
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.binaryMultitopic, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("Write to console"),
            mlModels = List(),
            strategy = Some(TestStrategies.testKafkaMetadata),
            triggerIntervalMs = None,
            options = Map()
          )
        ),
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
      structuredStreamingComponents =
        TestPipegraphs.AVRO.Structured.console.structuredStreamingComponents :::
          TestPipegraphs.AVRO.Structured.solr.structuredStreamingComponents :::
          TestPipegraphs.AVRO.Structured.elastic.structuredStreamingComponents :::
          TestPipegraphs.AVRO.Structured.hdfs.structuredStreamingComponents.map(
            _.copy(strategy = Some(
              StrategyModel(
                "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestErrorStrategy",
                None
              )
            )
            )
          ),
      dashboard = None
    )
  }

  object SparkSessionErrors {

    lazy val pipegraph = PipegraphModel(
      name = "TestStrategiesSettingSparkSessionConf",
      description =
        "Two strategies setting different spark.sql.shuffle.partitions, should create independent values for each one",
      owner = "user",
      isSystem = false,
      creationTime = System.currentTimeMillis,
      structuredStreamingComponents = List(
        StructuredStreamingETLModel(
          name = "TestSetShufflePartitionsTo10ETL",
          streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
          staticInputs = List.empty,
          streamingOutput = WriterModel.consoleWriter("Console Writer"),
          mlModels = List(),
          strategy = Some(
            StrategyModel(
              "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestSetShufflePartitionsTo10Strategy"
            )
          ),
          triggerIntervalMs = None,
          options = Map()
        ),
        StructuredStreamingETLModel(
          name = "TestSetShufflePartitionsTo20ETL",
          streamingInput = StreamingReaderModel.kafkaReader("Kafka Reader", TestTopicModel.avro, None),
          staticInputs = List.empty,
          streamingOutput = WriterModel.consoleWriter("Console Writer"),
          mlModels = List(),
          strategy = Some(
            StrategyModel(
              "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestSetShufflePartitionsTo20Strategy"
            )
          ),
          triggerIntervalMs = None,
          options = Map()
        )
      ),
      dashboard = None
    )

  }

  object MultiTopicReader {
    object Structured {

      lazy val kafkaMultitopicReadSame = PipegraphModel(
        name = "TestKafkaReaderMultitopicStructuredJSONPipegraph",
        description = "Test for reading from multiple topics for JSON topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "CopyFromTest1ToTest2jsonjson",
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.multitopicreadjson, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("console"),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )

      lazy val kafkaMultitopicReadDifferent = PipegraphModel(
        name = "TestKafkaReaderMultitopicStructuredJSONAvroPipegraph",
        description = "Test for reading from multiple topics for JSON and avro topics",
        owner = "user",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "CopyFromTest1ToTest2-avrojson",
            streamingInput =
              StreamingReaderModel.kafkaReaderMultitopic("Kafka Reader", TestTopicModel.multitopicreadjsonavro, None),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("console"),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )
    }

  }

  object RawReader {

    object Structured {
      val rateModel: RawModel = RawModel(
        name = "rate",
        uri = "",
        timed = false,
        schema = "",
        options = RawOptions(
          saveMode = "append",
          format = "rate",
          extraOptions = Some(Map[String, String]("rowsPerSecond" -> "10"))
        )
      )

      val fileModel: RawModel = RawModel(
        name = "fileWithRateValues",
        uri = "hdfs://" + System.getenv("HOSTNAME") + ":9000/user/root/raw_model_test/",
        timed = false,
        schema = StructType(
          List(
            StructField("timestamp", DataTypes.TimestampType),
            StructField("value", DataTypes.LongType)
          )
        ).json,
        options = RawOptions("append", "parquet", None, None)
      )
      val rateSourceToRawModel: PipegraphModel = PipegraphModel(
        name = "readKafkaWriteFileReadFileWriteConsole",
        description = "",
        owner = "",
        isSystem = false,
        creationTime = System.currentTimeMillis,
        structuredStreamingComponents = List(
          StructuredStreamingETLModel(
            name = "WriteRateToFile",
            streamingInput = StreamingReaderModel.rawReader("RateReader", rateModel),
            staticInputs = List.empty,
            streamingOutput = WriterModel.rawWriter("FileRateWriter", fileModel),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          ),
          StructuredStreamingETLModel(
            name = "ReadRateFromFile",
            streamingInput = StreamingReaderModel.rawReader("FileReader", fileModel),
            staticInputs = List.empty,
            streamingOutput = WriterModel.consoleWriter("console"),
            mlModels = List(),
            strategy = None,
            triggerIntervalMs = None,
            options = Map()
          )
        ),
        dashboard = None
      )
    }
  }
}
