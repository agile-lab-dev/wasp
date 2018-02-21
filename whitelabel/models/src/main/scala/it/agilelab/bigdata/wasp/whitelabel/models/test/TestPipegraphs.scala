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
                TestTopicModel.json.name,
                TestTopicModel.json.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map()
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
                TestTopicModel.json.name,
                TestTopicModel.json.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.solr.name,
              TestIndexModel.solr.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map()
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
                TestTopicModel.json.name,
                TestTopicModel.json.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nested.name,
              TestRawModel.nested.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map()
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
                TestTopicModel.json.name,
                TestTopicModel.json.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
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
                TestTopicModel.json.name,
                TestTopicModel.json.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.solr.name,
              TestIndexModel.solr.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
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
                TestTopicModel.json.name,
                TestTopicModel.json.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nested.name,
              TestRawModel.nested.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
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
                TestTopicModel.avro.name,
                TestTopicModel.avro.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map()
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
                TestTopicModel.avro.name,
                TestTopicModel.avro.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.solr.name,
              TestIndexModel.solr.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map()
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
                TestTopicModel.avro.name,
                TestTopicModel.avro.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nested.name,
              TestRawModel.nested.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
            config = Map()
          )
        ),
        rtComponents = List(),

        dashboard = None,
        isActive = false
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
          hdfs.structuredStreamingComponents,
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
                TestTopicModel.avro.name,
                TestTopicModel.avro.name
              )
            ),
            output = WriterModel.consoleWriter("myConsoleWriter"),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
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
                TestTopicModel.avro.name,
                TestTopicModel.avro.name
              )
            ),
            output = WriterModel.solrWriter(
              TestIndexModel.solr.name,
              TestIndexModel.solr.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
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
                TestTopicModel.avro.name,
                TestTopicModel.avro.name
              )
            ),
            output = WriterModel.rawWriter(
              TestRawModel.nested.name,
              TestRawModel.nested.name
            ),
            mlModels = List(),
            strategy = None,
            kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
          )
        ),
        structuredStreamingComponents = List(),
        rtComponents = List(),

        dashboard = None,
        isActive = false
      )
    }
  }

  object ERROR {

    lazy val multiETL = PipegraphModel(
      name = "TestErrorMultiEtlPipegraph",
      description = "Description of TestErrorMultiEtlAVROPipegraph",
      owner = "user",
      isSystem = false,
      creationTime = System.currentTimeMillis,

      legacyStreamingComponents = List(),
      structuredStreamingComponents =
        TestPipegraphs.AVRO.Structured.console.structuredStreamingComponents :::
        TestPipegraphs.AVRO.Structured.solr.structuredStreamingComponents :::
        TestPipegraphs.AVRO.Structured.hdfs.structuredStreamingComponents.map(
          _.copy(strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.whitelabel.test.ErrorStrategy", None)))),
      rtComponents = List(),

      dashboard = None,
      isActive = false
    )

  }
}