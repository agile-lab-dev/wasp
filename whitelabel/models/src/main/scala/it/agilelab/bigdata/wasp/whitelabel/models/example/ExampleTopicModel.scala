package it.agilelab.bigdata.wasp.whitelabel.models.example

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object ExampleTopicModel {

  val topic_name = "example"

  def apply() = TopicModel(
    name = TopicModel.name(topic_name),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  val topicSchema =
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
        |        }
        |""".stripMargin))
}