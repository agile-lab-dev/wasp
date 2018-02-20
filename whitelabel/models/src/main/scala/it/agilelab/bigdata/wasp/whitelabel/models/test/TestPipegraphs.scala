package it.agilelab.bigdata.wasp.whitelabel.models.test

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
            inputs = List(
              ReaderModel.kafkaReader(
                TestTopicModel.testJsonTopic.name,
                TestTopicModel.testJsonTopic.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map.empty
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
            inputs = List(
              ReaderModel.kafkaReader(
                TestTopicModel.testJsonTopic.name,
                TestTopicModel.testJsonTopic.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.index_name,
              TestIndexModel.index_name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map.empty
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
            inputs = List(
              ReaderModel.kafkaReader(
                TestTopicModel.testJsonTopic.name,
                TestTopicModel.testJsonTopic.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nestedSchemaRawModel.name,
              TestRawModel.nestedSchemaRawModel.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map.empty
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
      )
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
              ReaderModel.kafkaReader(
                TestTopicModel.testJsonTopic.name,
                TestTopicModel.testJsonTopic.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = Nil,
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
              ReaderModel.kafkaReader(
                TestTopicModel.testJsonTopic.name,
                TestTopicModel.testJsonTopic.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.index_name,
              TestIndexModel.index_name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = Nil,
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
              ReaderModel.kafkaReader(
                TestTopicModel.testJsonTopic.name,
                TestTopicModel.testJsonTopic.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nestedSchemaRawModel.name,
              TestRawModel.nestedSchemaRawModel.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = Nil,
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
            inputs = List(
              ReaderModel.kafkaReader(
                TestTopicModel.testAvroTopic.name,
                TestTopicModel.testAvroTopic.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map.empty
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
            inputs = List(
              ReaderModel.kafkaReader(
                TestTopicModel.testAvroTopic.name,
                TestTopicModel.testAvroTopic.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.index_name,
              TestIndexModel.index_name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map.empty
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
            inputs = List(
              ReaderModel.kafkaReader(
                TestTopicModel.testAvroTopic.name,
                TestTopicModel.testAvroTopic.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nestedSchemaRawModel.name,
              TestRawModel.nestedSchemaRawModel.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map.empty
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
      )
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
              ReaderModel.kafkaReader(
                TestTopicModel.testAvroTopic.name,
                TestTopicModel.testAvroTopic.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = Nil,
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
              ReaderModel.kafkaReader(
                TestTopicModel.testAvroTopic.name,
                TestTopicModel.testAvroTopic.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.index_name,
              TestIndexModel.index_name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = Nil,
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
              ReaderModel.kafkaReader(
                TestTopicModel.testAvroTopic.name,
                TestTopicModel.testAvroTopic.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nestedSchemaRawModel.name,
              TestRawModel.nestedSchemaRawModel.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = Nil,
        rtComponents = List(),

        dashboard = None,
        isActive = false
      )
    }
  }
}