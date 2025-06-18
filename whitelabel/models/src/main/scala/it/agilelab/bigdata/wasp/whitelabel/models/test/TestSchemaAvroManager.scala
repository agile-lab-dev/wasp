package it.agilelab.bigdata.wasp.whitelabel.models.test

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, JsonConverter}
import it.agilelab.bigdata.wasp.models.{
  KeyValueModel,
  KeyValueOption,
  PipegraphModel,
  ProducerModel,
  StrategyModel,
  StreamingReaderModel,
  StructuredStreamingETLModel,
  TopicModel,
  WriterModel
}
import org.apache.avro.{Schema, SchemaBuilder}

/** @author
  *   andreaL
  */

case class TopicAvro_v1(id: String, field1: Int, field2: Int)
case class TopicAvro_v2(id: String, field1: Int, field2: Int, field3: Option[String] = None)
case class TopicAvro_v3(id: String, field1: Int, field2: Int, field3: Option[String] = None)

case class AvroSchemaManagerHbase(id: String, test: TopicAvro_v2 /*test: TopicAvro_v1*/ )

object TestSchemaAvroManager {

  val schema_v1: Schema = SchemaBuilder
    .record("TopicAvro_v1")
    .fields()
    .name("id")
    .`type`()
    .stringType()
    .noDefault()
    .name("field1")
    .`type`()
    .intType()
    .noDefault()
    .name("field2")
    .`type`()
    .intType()
    .noDefault()
    .endRecord();

  val schema_v2: Schema = SchemaBuilder
    .record("TopicAvro_v2")
    .fields()
    .name("id")
    .`type`()
    .stringType()
    .noDefault()
    .name("field1")
    .`type`()
    .intType()
    .noDefault()
    .name("field2")
    .`type`()
    .intType()
    .noDefault()
    .name("field3")
    .`type`()
    .optional()
    .stringType()
    .endRecord();

  val schema_v3: Schema = SchemaBuilder
    .record("TopicAvro_v2")
    .fields()
    .name("id")
    .`type`()
    .stringType()
    .noDefault()
    .name("field1")
    .`type`()
    .intType()
    .noDefault()
    .name("field2")
    .`type`()
    .intType()
    .noDefault()
    .name("field3")
    .`type`()
    .optional()
    .stringType()
    .endRecord();

  private val topic_avro_v1 = "topic_avro_v1"
  private val topic_avro_v2 = "topic_avro_v2"
  private val topic_avro_v3 = "topic_avro_v3"

  lazy val topicAvro_v1 = TopicModel(
    name = TopicModel.name(topic_avro_v1),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = true,
    schema = JsonConverter
      .fromString(TestSchemaAvroManager.schema_v1.toString())
      .getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  lazy val topicAvro_v2 = topicAvro_v1.copy(
    name = TopicModel.name(topic_avro_v2),
    schema = JsonConverter
      .fromString(TestSchemaAvroManager.schema_v2.toString())
      .getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  lazy val topicAvro_v3 = topicAvro_v2.copy(
    name = TopicModel.name(topic_avro_v3),
    schema = JsonConverter
      .fromString(TestSchemaAvroManager.schema_v3.toString())
      .getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

  lazy val producer_v1 = ProducerModel(
    name = "TestProducerAvroSchemaManagerv1",
    className = "it.agilelab.bigdata.wasp.whitelabel.producers.TestProducerAvroSchemaManager_v1",
    topicName = Some(topicAvro_v1.name),
    isActive = false,
    configuration = None,
    isRemote = false,
    isSystem = false
  )

  lazy val producer_v2 = ProducerModel(
    name = "TestProducerAvroSchemaManagerv2",
    className = "it.agilelab.bigdata.wasp.whitelabel.producers.TestProducerAvroSchemaManager_v2",
    topicName = Some(topicAvro_v2.name),
    isActive = false,
    configuration = None,
    isRemote = false,
    isSystem = false
  )

  lazy val pipegraph = PipegraphModel(
    name = "TestPipegraphSchemaAvroManager",
    description = "Description of TestPipegraphSchemaAvroManager",
    owner = "user",
    isSystem = false,
    creationTime = System.currentTimeMillis,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(
        name = "ETL TestKafkaReaderWithDifferentSchemaAvro",
        streamingInput = StreamingReaderModel.kafkaReader(topicAvro_v3.name, topicAvro_v3, None),
        staticInputs = List.empty,
        streamingOutput = WriterModel.consoleWriter("Write to console TestKafkaReaderWithDifferentSchemaAvro"),
        mlModels = List(),
        strategy = Some(
          StrategyModel(
            className =
              "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestKafkaReaderWithDifferentVersionOfAvro",
            configuration = None
          )
        ),
        triggerIntervalMs = None,
        options = Map()
      ),
      StructuredStreamingETLModel(
        name = "ETL TestHBaseWithSchemaAvroManagerv1",
        streamingInput = StreamingReaderModel.kafkaReader(topicAvro_v2.name, topicAvro_v2, None),
        staticInputs = List.empty,
        streamingOutput = WriterModel.hbaseWriter(
          TestSchemaAvroManagerKeyValueModel.avroSchemaManagerHBaseModel.name,
          TestSchemaAvroManagerKeyValueModel.avroSchemaManagerHBaseModel
        ),
        mlModels = List(),
        strategy = Some(
          StrategyModel.create(
            className =
              "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestHBaseWithSchemaAvroManagerv2",
            configuration = ConfigFactory.parseString(""" v = "v2" """)
          )
        ),
        triggerIntervalMs = None,
        options = Map()
      )
    ),
    dashboard = None
  )

}

object TestSchemaAvroManagerKeyValueModel {

  lazy val avroSchemaManagerHBaseModel = TestSchemaAvroManagerhHBaseModel()
  lazy val namespace                   = "test"
  lazy val tableName: String           = ConfigManager.conf.getString("environment.prefix") + "schemaAvroManager"
  private[wasp] object TestSchemaAvroManagerhHBaseModel {

    val name              = "TestSchemaAvroManagerHBaseModel"
    val completeTableName = s"$namespace:$tableName"

    val schema: Schema = TestSchemaAvroManager.schema_v2

    val dfFieldsSchema: String =
      s"""
         |"id":{"cf":"rowkey", "col":"key", "type":"string"},
         |"test":{"cf":"c", "col":"test", "avro":"topic_avro_v2"}
         |""".stripMargin

    private val hbaseSchema = KeyValueModel.generateField(namespace, tableName, Some(dfFieldsSchema))

    def apply() = KeyValueModel(
      name,
      hbaseSchema,
      None,
      Some(
        Seq(
          KeyValueOption("hbase.spark.config.location", "/etc/hbase/conf/hbase-site.xml"),
          KeyValueOption("newtable", "5"),
          KeyValueOption("topic_avro_v2", schema.toString())
        )
      ),
      true,
      None
    )

  }

}
