package it.agilelab.bigdata.wasp.whitelabel.models.example

import it.agilelab.bigdata.wasp.core.models._

private[wasp] object ExamplePipegraphModel {

  lazy val pipegraph = PipegraphModel(
    name = "ExamplePipegraph",
    description = "Description of Example Pipegraph",
    owner = "user",
    isSystem = false,
    creationTime = System.currentTimeMillis,

    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(
        name = "Write on console",
        inputs = List(
          ReaderModel.kafkaReader(
            ExampleTopicModel.topic.name,
            ExampleTopicModel.topic.name
          )
        ),
        output = WriterModel.consoleWriter("console-writer"),
        mlModels = List.empty,
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map()
      )
    ),
    rtComponents = List(),

    dashboard = None,
    isActive = false)
}