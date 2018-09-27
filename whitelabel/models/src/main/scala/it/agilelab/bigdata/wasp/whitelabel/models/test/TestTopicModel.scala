package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object TestTopicModel {

  private val topic_name = "test"
  private val topic2_name = "test2"
  private val topicCheckpoint_name = "testCheckpoint"

  lazy val json = TopicModel(name = TopicModel.name(topic_name + "_json"),
                             creationTime = System.currentTimeMillis,
                             partitions = 3,
                             replicas = 1,
                             topicDataType = "json",
                             keyFieldName = None,
                             headersFieldName = None,
                             schema = JsonConverter
                               .fromString(topicSchema)
                               .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val jsonWithMetadata = TopicModel(name = TopicModel.name(topic_name + "_with_metadata_json"),
                                         creationTime = System.currentTimeMillis,
                                         partitions = 3,
                                         replicas = 1,
                                         topicDataType = "json",
                                         keyFieldName = None,
                                         headersFieldName = None,
                                         schema = JsonConverter
                                           .fromString(withMetadataSchema)
                                           .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val json2 = TopicModel(name = TopicModel.name(topic2_name + "_json"),
                              creationTime = System.currentTimeMillis,
                              partitions = 3,
                              replicas = 1,
                              topicDataType = "json",
                              keyFieldName = Some("nested.field3"),
                              headersFieldName = None,
                              schema = JsonConverter
                                .fromString(topicSchema)
                                .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val jsonCheckpoint = TopicModel(name = TopicModel.name(topicCheckpoint_name + "_json"),
                                       creationTime = System.currentTimeMillis,
                                       partitions = 3,
                                       replicas = 1,
                                       topicDataType = "json",
                                       keyFieldName = None,
                                       headersFieldName = None,
                                       schema = JsonConverter
                                         .fromString(topicCheckpointSchema)
                                         .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val avro = TopicModel(name = TopicModel.name(topic_name + "_avro"),
                             creationTime = System.currentTimeMillis,
                             partitions = 3,
                             replicas = 1,
                             topicDataType = "avro",
                             keyFieldName = None,
                             headersFieldName = None,
                             schema = JsonConverter
                               .fromString(topicSchema)
                               .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val avro2 = TopicModel(name = TopicModel.name(topic2_name + "_avro"),
                              creationTime = System.currentTimeMillis,
                              partitions = 3,
                              replicas = 1,
                              topicDataType = "avro",
                              keyFieldName = Some("nested.field3"),
                              headersFieldName = None,
                              schema = JsonConverter
                                .fromString(topicSchema)
                                .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val avroCheckpoint = TopicModel(name = TopicModel.name(topicCheckpoint_name + "_avro"),
                                       creationTime = System.currentTimeMillis,
                                       partitions = 3,
                                       replicas = 1,
                                       topicDataType = "avro",
                                       keyFieldName = None,
                                       None,
                                       schema = JsonConverter
                                         .fromString(topicCheckpointSchema)
                                         .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  private val topicSchema =
    TopicModel.generateField("test", "test", Some(
      """
        |        {
        |            "name": "id",
        |            "type": "string",
        |            "doc": ""
        |        },
        |        {
        |            "name": "number",
        |            "type": "int",
        |            "doc": ""
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
      """.stripMargin))

  private val withMetadataSchema =
    TopicModel.generateField("test", "test", Some(
      """
                {
                  "name": "metadata",
                  "type": {
                    "name": "nested_metadata",
                    "type": "record",
                    "fields": [
                      {
                        "name": "id",
                        "type": "string"
                      },
                      {
                        "name": "sourceId",
                        "type": "string"
                      },
                      {
                        "name": "arrivalTimestamp",
                        "type": "long"
                      },
                      {
                        "name": "lastSeenTimestamp",
                        "type": "long"
                      },
                      {
                        "name": "path",
                        "type": {
                          "type": "array",
                          "items": {
                            "name": "Path",
                            "type": "record",
                            "fields": [
                              {
                                "name": "name",
                                "type": "string"
                              },
                              {
                                "name": "ts",
                                "type": "long"
                              }
                            ]
                          }
                        }
                      }
                    ]
                  }
                },
                {
                  "name": "id",
                  "type": "string",
                  "doc": ""
                },
                {
                  "name": "number",
                  "type": "int",
                  "doc": ""
                },
                {
                  "name": "nested",
                  "type": {
                    "type": "record",
                    "name": "nested_document",
                    "fields": [
                      {
                        "name": "field1",
                        "type": "string"
                      },
                      {
                        "name": "field2",
                        "type": "long"
                      },
                      {
                        "name": "field3",
                        "type": "string"
                      }
                    ]
                  }
                }"""))

  private val topicCheckpointSchema =
    TopicModel.generateField("test", "test", Some(
      """
        |        {
        |            "name": "version",
        |            "type": "string",
        |            "doc": ""
        |        },
        |        {
        |            "name": "id",
        |            "type": "string",
        |            "doc": ""
        |        },
        |        {
        |            "name": "value",
        |            "type": "int",
        |            "doc": ""
        |        },
        |        {
        |            "name": "sum",
        |            "type": "int",
        |            "doc": ""
        |        },
        |        {
        |            "name": "oldSumInt",
        |            "type": "int",
        |            "doc": ""
        |        }
        |        {
        |            "name": "oldSumString",
        |            "type": "string",
        |            "doc": ""
        |        }
      """.stripMargin))
}