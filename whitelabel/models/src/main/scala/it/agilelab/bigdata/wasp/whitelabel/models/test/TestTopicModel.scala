package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object TestTopicModel {

  val topic_name = "test"

  lazy val testTopic = TopicModel(
    name = TopicModel.name(topic_name),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    partitionKeyField = None,
    schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  val topicSchema =
    TopicModel.generateField("test", "test", Some(
      """{
        |            "name": "id",
        |            "type": "string",
        |            "doc": ""
        |        },
        |        {
        |            "name": "number",
        |            "type": "int",
        |            "doc": ""
        |
        |        },
        |        {
        |            "name": "nested",
        |            "type" : {
        |                         "type" : "record",
        |                         "name" : "nested_document",
        |                         "fields" : [
        |                            {"name" : "field1",
        |                             "type" : "string"},
        |
        |                            {"name" : "field2",
        |                             "type" : "long"},
        |
        |                            {"name" : "field3",
        |                             "type" : "string"}
        |                          ]
        |                      }
        |        }
        |""".stripMargin))
}