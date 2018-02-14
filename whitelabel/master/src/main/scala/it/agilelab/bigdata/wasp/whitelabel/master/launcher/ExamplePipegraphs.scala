package it.agilelab.bigdata.wasp.whitelabel.master.launcher

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

object ExamplePipegraphs {

  lazy val examplePipegraphName = "ExamplePipegraph"
  lazy val exampleTopic = ExampleTopic()
  lazy val examplePipegraph = ExamplePipegraph()
}

private object ExampleTopic {

  val topic_name = "example"

  def apply() = TopicModel(
    name = TopicModel.name(topic_name),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  val topicSchema = s"${
    TopicModel.generateField("example", "example", Some(
      """{
        |            "name": "banana",
        |            "type": "string",
        |            "doc": "First Name of Customer"
        |        },
        |        {
        |            "name": "pigiama",
        |            "type": "string",
        |            "doc": "Last Name of Customer"
        |        }""".stripMargin))
  }"
}

private object ExamplePipegraph {

  def apply() = PipegraphModel(
    name = ExamplePipegraphs.examplePipegraphName,
    description = "System Example Pipegraph",
    owner = "system",
    isSystem = true,
    creationTime = System.currentTimeMillis,
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(
        name = "write on console",
        inputs = List(ReaderModel.kafkaReader(ExamplePipegraphs.exampleTopic.name, ExamplePipegraphs.exampleTopic.name)),
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