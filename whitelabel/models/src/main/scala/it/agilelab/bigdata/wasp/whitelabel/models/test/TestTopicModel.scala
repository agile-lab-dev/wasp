package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.utils.JsonConverter
import it.agilelab.bigdata.wasp.models.{MultiTopicModel, TopicCompression, TopicModel}

private[wasp] object TestTopicModel {

  private val topic_name = "test"
  private val topic2_name = "test2"
  private val topic3_name = "test3"
  private val topic4_name = "test4"
  private val topic5_name = "test5"
  private val topic6_name = "test6"
  private val topicCheckpoint_name = "testCheckpoint"

  lazy val monitoring = TopicModel(
    name = TopicModel.name("monitoring"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "plaintext",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = false,
    schema = org.mongodb.scala.bson.BsonDocument())

  lazy val json = TopicModel(name = TopicModel.name(topic_name + "_json"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = false,
    schema = JsonConverter
      .fromString(topicSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val json6 = TopicModel(name = TopicModel.name(topic6_name + "_json"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = false,
    schema = JsonConverter
      .fromString(topicSchemaHbaseMultipleClustering)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val jsonWithMetadata = TopicModel(name = TopicModel.name(topic_name + "_with_metadata_json"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = false,
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
    valueFieldsNames = None,
    useAvroSchemaManager = false,
    schema = JsonConverter
      .fromString(topicSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val json3 = json.copy(name = TopicModel.name(topic3_name + "_json"))

  lazy val json4 = json.copy(name = TopicModel.name(topic4_name + "_json"),
    valueFieldsNames = Some(List("id", "number", "nested")))

  lazy val json5 = json.copy(name = TopicModel.name(topic5_name + "_json"),
    valueFieldsNames = Some(List("id", "number", "nested")))

  lazy val jsonMultitopicRead = MultiTopicModel.fromTopicModels("multitopic_read_json",
    "topic",
    Seq(json, json3))


  lazy val jsonMultitopicWrite = MultiTopicModel.fromTopicModels("multitopic_write_json",
    "topic",
    Seq(json4, json5))

  lazy val json2ForKafkaHeaders = TopicModel(name = TopicModel.name(topic2_name + "_json"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = Some("nested.field3"),
    headersFieldName = Some("headers"),
    valueFieldsNames = Some(List("id", "number", "nested")),
    useAvroSchemaManager = false,
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
    valueFieldsNames = None,
    useAvroSchemaManager = false,
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
    valueFieldsNames = None,
    useAvroSchemaManager = true,
    schema = JsonConverter
      .fromString(topicSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()),
    topicCompression = TopicCompression.Disabled)

  lazy val avro2 = TopicModel(name = TopicModel.name(topic2_name + "_avro"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("nested.field3"),
    headersFieldName = None,
    valueFieldsNames = Some(List("id", "number", "nested")),
    useAvroSchemaManager = true,
    schema = JsonConverter
      .fromString(topicSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()),
    topicCompression = TopicCompression.Lz4)

  lazy val avro3 = avro.copy(name = TopicModel.name(topic3_name + "_avro"))

  lazy val avro4 = avro.copy(name = TopicModel.name(topic4_name + "_avro"),
    valueFieldsNames = Some(List("id", "number", "nested")))

  lazy val avro5 = avro.copy(name = TopicModel.name(topic5_name + "_avro"),
    valueFieldsNames = Some(List("id", "number", "nested")))

  lazy val avroMultitopicRead = MultiTopicModel.fromTopicModels("multitopic_read_avro",
    "topic",
    Seq(avro, avro3))

  lazy val avroMultitopicWrite = MultiTopicModel.fromTopicModels("multitopic_write_avro",
    "topic",
    Seq(avro4, avro5))

  lazy val avro2ForKafkaHeaders = TopicModel(name = TopicModel.name(topic2_name + "headers_avro"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("nested.field3"),
    headersFieldName = Some("headers"),
    valueFieldsNames = Some(List("id", "number", "nested")),
    useAvroSchemaManager = true,
    schema = JsonConverter
      .fromString(topicSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val avroCheckpoint = TopicModel(name = TopicModel.name(topicCheckpoint_name + "_avro"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = true,
    schema = JsonConverter
      .fromString(topicCheckpointSchema)
      .getOrElse(org.mongodb.scala.bson.BsonDocument()))

  lazy val plaintext1 = TopicModel(name = TopicModel.name(topic_name + "_plaintext"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "plaintext",
    keyFieldName = Some("myKey"),
    headersFieldName = Some("myHeaders"),
    valueFieldsNames = Some(List("myValue")),
    useAvroSchemaManager = false,
    schema = org.mongodb.scala.bson.BsonDocument())

  lazy val plaintext2 = plaintext1.copy(name = TopicModel.name(topic2_name + "_plaintext"))

  lazy val plaintextMultitopic = MultiTopicModel.fromTopicModels("multitopic_plaintext",
    "myTopic",
    Seq(plaintext1, plaintext2))

  lazy val binary1 = TopicModel(name = TopicModel.name(topic_name + "_binary"),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "binary",
    keyFieldName = Some("myKey"),
    headersFieldName = Some("myHeaders"),
    valueFieldsNames = Some(List("myValue")),
    useAvroSchemaManager = false,
    schema = org.mongodb.scala.bson.BsonDocument())

  lazy val binary2 = binary1.copy(name = TopicModel.name(topic2_name + "_binary"))

  lazy val binaryMultitopic = MultiTopicModel.fromTopicModels("multitopic_binary",
    "myTopic",
    Seq(binary1, binary2))

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

  private val topicSchemaHbaseMultipleClustering =
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
        |        },
        |        {
        |            "name": "number_clustering",
        |            "type": "int",
        |            "doc": ""
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
