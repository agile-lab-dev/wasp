package it.agilelab.bigdata.wasp.whitelabel.models.example

import it.agilelab.bigdata.wasp.core.models._

private[wasp] object ExamplePipegraphModel {

  val examplePipegraphName = "ExamplePipegraph"

  lazy val examplePipegraph = PipegraphModel(
    name = examplePipegraphName,
    description = "System Example Pipegraph",
    owner = "system",
    isSystem = false,
    creationTime = System.currentTimeMillis,
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(
        name = "write on console",
        inputs = List(
          ReaderModel.kafkaReader(
            ExampleTopicModel.exampleTopic.name,
            ExampleTopicModel.exampleTopic.name
          )
        ),
        output = WriterModel.consoleWriter("console-writer"),
        mlModels = List.empty,
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map.empty
      )
    ),
    rtComponents = Nil,
    dashboard = None,
    isActive = false)
}