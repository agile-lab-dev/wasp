package it.agilelab.bigdata.wasp.whitelabel.models.example

import it.agilelab.bigdata.wasp.core.models._

private[wasp] object ExamplePipegraphModel {

  def apply() = PipegraphModel(
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
            ExampleTopicModel.topic_name,
            ExampleTopicModel.topic_name
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