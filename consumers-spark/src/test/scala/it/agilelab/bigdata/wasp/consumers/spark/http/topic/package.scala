package it.agilelab.bigdata.wasp.consumers.spark.http

import com.sksamuel.avro4s.AvroSchema
import it.agilelab.bigdata.wasp.consumers.spark.http.data.{FromKafka, SampleData}
import it.agilelab.bigdata.wasp.core.utils.JsonConverter
import it.agilelab.bigdata.wasp.models.TopicModel

package object topic {
  val fromKafkaInputTopic: TopicModel = TopicModel(
    name = "fromkafka-in.topic",
    creationTime = System.currentTimeMillis(),
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = false,
    schema = JsonConverter
      .fromString(AvroSchema[FromKafka].toString(false))
      .getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  val sampleDataOutputTopic: TopicModel = TopicModel(
    name = "sample-data-out.topic",
    creationTime = System.currentTimeMillis(),
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = false,
    schema = JsonConverter
      .fromString(AvroSchema[SampleData].toString(false))
      .getOrElse(org.mongodb.scala.bson.BsonDocument())
  )
}
