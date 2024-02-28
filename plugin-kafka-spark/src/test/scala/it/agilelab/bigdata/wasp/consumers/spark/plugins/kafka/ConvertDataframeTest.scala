package it.agilelab.bigdata.wasp.consumers.spark.plugins.kafka

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.core.utils.AvroSchemaConverters
import it.agilelab.bigdata.wasp.models.{MultiTopicModel, SubjectStrategy, TopicCompression, TopicModel}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.FlatSpec
import spray.json.{JsObject, JsString, JsValue}

import java.nio.charset.StandardCharsets

class ConvertDataframeTest extends FlatSpec with SparkSuite with ConvertDataframeTestData {

  import spark.implicits._

  val topicFieldName = Some("topic")

  val darwinConf = ConfigFactory.parseString("""
      |type: cached_eager
      |connector: "mock"
      |endianness: "LITTLE_ENDIAN"
        """.stripMargin)

  val nameTopicA = "TOPIC-A"
  val nameTopicB = "TOPIC-B"

  it should "handle nested value fields with TopicModels with dataType avro" in {

    val topics          = Seq(topicRight, topicLeft)
    val multiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      dfForNestedValues.toDF(),
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )
    assert(result.count() === 2)
  }

  it should "handle nested value fields with TopicModels with dataType json" in {

    val jsonTopicRight = topicRight.copy(
      topicDataType = "json",
      schema = BsonDocument(
        AvroSchemaConverters.convertStructToAvro(schemaRight, SchemaBuilder.builder().record("schema"), "wasp").toString
      )
    )
    val jsonTopicLeft = topicLeft.copy(
      topicDataType = "json",
      schema = BsonDocument(
        AvroSchemaConverters.convertStructToAvro(schemaLeft, SchemaBuilder.builder().record("schema"), "wasp").toString
      )
    )

    val topics          = Seq(jsonTopicRight, jsonTopicLeft)
    val multiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      dfForNestedValues.toDF(),
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )

    import org.apache.spark.sql.functions._
    assert(
      result
        .select(col("value").cast(StringType))
        .as[String]
        .first() === """{"name":"john","surname":"travolta"}"""
    )

  }

  it should "correctly work with topic models having different schema, different TopicDataType and one of them has headers field set " in {
    val topics = Seq(
      testTopicModelErrorPerson_avro_key.copy(name = nameTopicB), // avro, keyField
      testTopicModelPerson_json_keyValueHeaders.copy(name = nameTopicA)
    ) // json, keyField, valueFields
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithDifferentSchemaWithHeaders,
      topicFieldName,
      topics,
      multiTopicModel,
      Some(darwinConf)
    )

    val resultRows = result.as[KafkaOutput].collect()
    assert(resultRows.size === testDfTopicModelWithDifferentSchemaWithHeaders.collect().size)
    val forTopicA = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicA)

    assert(forTopicA.head.value === """{"name":"pippo","surname":"franco"}""")
    assert(forTopicA.last.value === """{"name":"pluto","surname":"franco"}""")

  }

  it should "correctly work with topic models having different schema, one of them has null topic keyfield " in {
    val topics = Seq(
      testTopicModelErrorPerson_avro_key.copy(name = nameTopicB, keyFieldName = None), // avro, keyField
      testTopicModelPerson_json_keyValueHeaders.copy(name = nameTopicA)
    ) // json, keyField, valueFields
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithDifferentSchemaWithHeaders,
      topicFieldName,
      topics,
      multiTopicModel,
      Some(darwinConf)
    )
    testDfTopicModelWithDifferentSchemaWithHeaders.show
result.show
    val resultRows = result.as[KafkaOutput].collect()
    assert(resultRows.size === testDfTopicModelWithDifferentSchemaWithHeaders.collect().size)
    val forTopicA = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicA)
    val forTopicB = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicB)

    assert(forTopicA.forall(_.key != null))
    assert(forTopicB.forall(_.key == null))

  }

  it should "correctly work with topic models having different schema and TopicDataType avro" in {

    val topics = Seq(
      testTopicModelPerson_avro_keyKeySchema_2
        .copy(name = nameTopicA, useAvroSchemaManager = true),   // avro, keyField, keySchema, schemaManager
      testTopicModelErrorPerson_avro_key.copy(name = nameTopicB) // avro, keyField
    )
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithDifferentSchema,
      topicFieldName,
      topics,
      multiTopicModel,
      Some(darwinConf)
    )
    val resultRows = result.as[KafkaOutput].collect()

    assert(resultRows.size === testDfTopicModelWithDifferentSchema.collect().size)
    val forTopic2A = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicA)
    val forTopic2B = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicB)

    assert(forTopic2A.length === 2)
    assert(forTopic2B.length === 2)

  }

  it should "correctly work with topic models having different schema and TopicDataType json" in {

    val topics = Seq(
      testTopicModelPerson_json_key_1.copy(name = nameTopicA),   // json, keyField
      testTopicModelErrorPerson_json_key.copy(name = nameTopicB) // json, keyField
    )
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithDifferentSchema,
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )

    val resultRows = result.as[KafkaOutput].collect()

    assert(resultRows.size === testDfTopicModelWithDifferentSchema.collect().size)
    val forTopic2A = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicA)
    val forTopic2B = resultRows.filter(kafkaOutput => kafkaOutput.topic == nameTopicB)

    assert(forTopic2A.head.key.sameElements(Array(104.toByte, 101.toByte, 121.toByte)))
    assert(forTopic2A.head.value === s"""{"name":"pippo","surname":"franco"}""")
    assert(forTopic2A.last.key.sameElements(Array(98.toByte, 117.toByte, 104.toByte)))
    assert(forTopic2A.last.value === s"""{"name":"pluto","surname":"franco"}""")

    assert(forTopic2B.head.key.sameElements(Array(65.toByte, 65.toByte, 50.toByte, 51.toByte)))
    assert(
      forTopic2B.head.value === s"""{"name":"mickey","surname":"mouse","error":"ErrorMsg"}"""
    )
    assert(forTopic2B.last.key.sameElements(Array(65.toByte, 65.toByte, 52.toByte, 52.toByte)))
    assert(
      forTopic2B.last.value === s"""{"name":"donald","surname":"duck","error":"ErrorMsg"}"""
    )

  }

  it should "correctly work with topic models having the same schema, TopicDataType avro and at least one one of them uses the schema manager " in {

    val topics = Seq(
      testTopicModelPerson_avro_keyKeySchema_1
        .copy(name = "TOPIC-A", useAvroSchemaManager = true),                           // avro, keyField, keySchema, schemaManager
      testTopicModelPerson_avro_key.copy(name = "TOPIC-B", useAvroSchemaManager = true) // avro, keyField, schemaManager
    )
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithSameSchema.toDF(),
      topicFieldName,
      topics,
      multiTopicModel,
      Some(darwinConf)
    )

    val resultRows = result.as[KafkaOutput].collect()
    assert(resultRows.head.topic === "TOPIC-A")
    assert(
      resultRows.head.key.sameElements(
        Array(
          -61.toByte,
          1.toByte,
          -113.toByte,
          92.toByte,
          57.toByte,
          63.toByte,
          26.toByte,
          -43.toByte,
          117.toByte,
          114.toByte,
          2.toByte
        )
      )
    )

    assert(resultRows(1).topic === "TOPIC-B")
    assert(resultRows.last.key.sameElements(Array(108.toByte, 117.toByte, 99.toByte, 97.toByte)))

  }

  it should "correctly work with topic models having the same schema and TopicDataType avro" in {

    val topics = Seq(
      testTopicModelPerson_avro_keyKeySchema_1.copy(name = "TOPIC-A"), // avro, keyField, keySchema
      testTopicModelPerson_avro_key.copy(name = "TOPIC-B")             // avro, keyField
    )
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithSameSchema.toDF(),
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )

    val resultRows = result.as[KafkaOutput].collect()
    assert(resultRows.head.topic === "TOPIC-A")
    assert(resultRows.head.key.sameElements(Array(2.toByte)))

    assert(resultRows(1).topic === "TOPIC-B")
    assert(resultRows.last.key.sameElements(Array(108.toByte, 117.toByte, 99.toByte, 97.toByte)))

  }

  it should "correctly work with topic models having the same schema and TopicDataType json" in {

    val topicWithValueFieldsAndStringKey1 = // json, keyField, valueFields
      testTopicModelPerson_json_key_1.copy(
        valueFieldsNames = Some(Seq("name", "surname"))
      )

    val topicWithValueFieldsAndStringKey2 = // json, keyField, valueFields
      testTopicModelPerson_json_key_1.copy(
        name = "TOPIC-B",
        valueFieldsNames = Some(Seq("name", "surname"))
      )

    val topics = Seq(
      topicWithValueFieldsAndStringKey1,
      topicWithValueFieldsAndStringKey2
    )
    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels(
      "name",
      "topic",
      Seq(topicWithValueFieldsAndStringKey1, topicWithValueFieldsAndStringKey2)
    )

    val result = KafkaWriters.convertDataframe(
      testDfTopicModelWithSameSchemaStringKey.toDF(),
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )

    val resultRows = result.as[KafkaOutput].collect()
    assert(resultRows.head.topic === "TOPIC-A")
    assert(resultRows.head.value === """{"name":"mario","surname":"rossi"}""")

    assert(resultRows(1).topic === "TOPIC-B")
    assert(resultRows(1).value === """{"name":"luca","surname":"bobo"}""")

  }

  it should "throw an exception when the schema does not contain the fields set as value fields" in {

    val topicModel1 = testTopicModelPerson_avro_keyKeySchema_1.copy(
      name = "TOPIC-A",
      valueFieldsNames = Some(Seq("name", "surname", "NOT-VALID"))
    )
    val topicModel2 = testTopicModelPerson_json_key_1.copy(name = "TOPIC-B")

    val topics = Seq(
      topicModel1,
      topicModel2
    )
    val multiTopicModel: MultiTopicModel =
      MultiTopicModel.fromTopicModels("name", "topic", Seq(topicModel1, topicModel2))

    val df = Seq(
      (Some("A1"), "sergio", "castellitto", "TOPIC-A"),
      (None, "marco", "giallini", "TOPIC-B")
    ).toDF("id", "name", "surname", "topic")

    val caught =
      intercept[IllegalArgumentException] {
        KafkaWriters.convertDataframe(
          df,
          topicFieldName,
          topics,
          multiTopicModel,
          None
        )
      }
    assert(
      caught.getMessage.startsWith(
        """Fields specified in valueFieldsNames of topic TOPIC-A [name,surname,NOT-VALID] are not present in the df schema"""
      )
    )

  }

  it should "throw an exception when the TopicModel schema is defined and some of its fields are not set in the definition of the model as headers, nor key, nor part of the value. " in {

    val topicModel1 = testTopicModelPerson_avro_keyKeySchema_1.copy(
      name = "TOPIC-A",
      valueFieldsNames = Some(Seq("name")),
      keySchema = Some(SchemaBuilder.builder().stringBuilder().endString().toString)
    ) //topic and surname missing
    val topicModel2 = testTopicModelPerson_json_key_1.copy(name = "TOPIC-B", valueFieldsNames = Some(Seq("surname"))) //topic and name missing

    val topics = Seq(
      topicModel1,
      topicModel2
    )
    val multiTopicModel: MultiTopicModel =
      MultiTopicModel.fromTopicModels("name", "topic", Seq(topicModel1, topicModel2))

    val df = Seq(
      (Some("boom"), Some("sergio"), None),
      (None, None, Some("giallini"))
    ).toDF("id", "name", "surname")

    val caught =
      intercept[IllegalArgumentException] {
        KafkaWriters.convertDataframe(
          df,
          topicFieldName,
          topics,
          multiTopicModel,
          None
        )
      }
    assert(
      caught.getMessage === """Expected column named `topic` for topic TOPIC-A to be used as topic, but found None: Cannot resolve column name "topic" among (id, name, surname);"""
    )

  }

  it should "throw an exception when the schema does not contain the fields set as key fields" in {

    val topicModel1 = testTopicModelPerson_avro_keyKeySchema_1.copy(name = "TOPIC-A", keyFieldName = Some("id-WRONG"))
    val topicModel2 = testTopicModelPerson_json_key_1.copy(name = "TOPIC-B")

    val topics = Seq(
      topicModel1,
      topicModel2
    )
    val multiTopicModel: MultiTopicModel =
      MultiTopicModel.fromTopicModels("name", "topic", Seq(topicModel1, topicModel2))

    val df = Seq(
      (Some("A1"), "sergio", "castellitto", "TOPIC-A"),
      (None, "marco", "giallini", "TOPIC-B")
    ).toDF("id", "name", "surname", "topic")

    val caught =
      intercept[IllegalArgumentException] {
        KafkaWriters.convertDataframe(
          df,
          topicFieldName,
          topics,
          multiTopicModel,
          None
        )
      }

    assert(
      caught.getMessage === s"""Expected column named `id-WRONG` for topic TOPIC-A to be used as key, but found None: Cannot resolve column name "id-WRONG" among (id, name, surname, topic);"""
    )
  }

  it should "correctly work with three topic models, all different in everything" in {

    val topicModel1 = testTopicModelErrorPerson_avro_key.copy(name = "TOPIC-1")
    val topicModel2 = testTopicModelPerson_json_key_2.copy(name = "TOPIC-2")
    val topicModel3 = testTopicModelAddress_avro_keyKeySchemaValueHeaders.copy(name = "TOPIC-3")

    val df = Seq(
      (Some("123"), Some("mario"), Some("biondi"), Some("404"), Some("TOPIC-1"), None, None, None),
      (None, Some("luca"), Some("marchegiani"), None, Some("TOPIC-2"), Some("hello"), None, None),
      (None, None, None, None, Some("TOPIC-3"), Some("000"), Some("add"), Some("head"))
    ).toDF("key", "name", "surname", "error", "topic", "id", "address", "headers")

    val topics = Seq(
      topicModel1,
      topicModel2,
      topicModel3
    )
    val multiTopicModel: MultiTopicModel =
      MultiTopicModel.fromTopicModels("name", "topic", Seq(topicModel1, topicModel2, topicModel3))

    val result = KafkaWriters.convertDataframe(
      df,
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )

    val resultRows = result.as[KafkaOutput].collect()

    //check keys
    assert(resultRows(0).key.sameElements(Array(49.toByte, 50.toByte, 51.toByte)))
    assert(resultRows(1).key.sameElements(Array(104.toByte, 101.toByte, 108.toByte, 108.toByte, 111.toByte)))
    assert(resultRows(2).key.sameElements(Array(6.toByte, 48.toByte, 48.toByte, 48.toByte)))

    //check topic
    assert(resultRows(0).topic === "TOPIC-1")
    assert(resultRows(1).topic === "TOPIC-2")
    assert(resultRows(2).topic === "TOPIC-3")

  }

  it should "correctly work with a TopicModel whose dataType is binary and another one whose dataType is json. " in {

    val topicModel1 = testTopicModelError_binary.copy(name = "TOPIC-A")
    val topicModel2 = testTopicModelPerson_json_key_2.copy(name = "TOPIC-B")

    val df = Seq(
      (Some("hello"), Some("giovanni"), Some("bianchi"), "TOPIC-B", None, None),
      (None, None, None, "TOPIC-A", Some("test"), Some("errorMsg".getBytes))
    ).toDF("id", "name", "surname", "topic", "key", "error")

    val topics = Seq(topicModel1, topicModel2)

    val multiTopicModel: MultiTopicModel = MultiTopicModel.fromTopicModels("name", "topic", topics)

    val result = KafkaWriters.convertDataframe(
      df,
      topicFieldName,
      topics,
      multiTopicModel,
      None
    )

    val resultRows = result.as[KafkaOutput].collect()
    assert(resultRows.head.topic === "TOPIC-B")
    assert(resultRows.last.topic === "TOPIC-A")
    assert(new String(resultRows.last.value) === "errorMsg")
    assert(new String(resultRows.head.key) === "hello")

  }

  // plaintext single

  it should "convert in case of single plaintext topic with header and key" in {
    val tm    = TopicModel.plainText("name", 0L, 1, 1, Some("_1"), Some("_2"), Some("_3"), TopicCompression.Disabled)
    val input = ("key", "header", "test")
    val res = KafkaWriters.convertDataframe(
      spark.createDataset(List(input)).toDF(),
      None,
      Seq.empty,
      tm,
      None
    )
    assert(
      res
        .selectExpr("key as _1", "headers as _2", "value as _3")
        .as[(String, String, String)]
        .collect() sameElements List(input)
    )
  }

  it should "convert in case of single plaintext topic with header" in {
    val tm    = TopicModel.plainText("name", 0L, 1, 1, None, Some("_1"), Some("_2"), TopicCompression.Disabled)
    val input = ("header", "test")
    val res = KafkaWriters.convertDataframe(
      spark.createDataset(List(input)).toDF(),
      None,
      Seq.empty,
      tm,
      None
    )
    assert(
      res
        .selectExpr("headers as _1", "value as _2")
        .as[(String, String)]
        .collect() sameElements List(input)
    )
  }

  it should "convert in case of single plaintext topic with only value" in {
    val tm    = TopicModel.plainText("name", 0L, 1, 1, None, None, None, TopicCompression.Disabled)
    val input = "test"
    val res = KafkaWriters.convertDataframe(
      spark.createDataset(List(input)).toDF(),
      None,
      Seq.empty,
      tm,
      None
    )
    assert(res.as[String].collect() sameElements List(input))
  }

  // binary single

  it should "convert in case of single binary topic with header and key" in {
    val tm    = TopicModel.binary("name", 0L, 1, 1, Some("_1"), Some("_2"), Some("_3"), TopicCompression.Disabled)
    val input = ("key", "header", "test".getBytes(StandardCharsets.UTF_8))
    val res = KafkaWriters.convertDataframe(
      spark.createDataset(List(input)).toDF(),
      None,
      Seq.empty,
      tm,
      None
    )
    assert(
      res
        .selectExpr("key as _1", "headers as _2", "cast (value as string) as _3")
        .as[(String, String, String)]
        .collect() sameElements List(input.copy(_3 = new String(input._3, StandardCharsets.UTF_8)))
    )
  }

  it should "convert in case of single binary topic with header" in {
    val tm    = TopicModel.binary("name", 0L, 1, 1, None, Some("_1"), Some("_2"), TopicCompression.Disabled)
    val input = ("header", "test".getBytes(StandardCharsets.UTF_8))
    val res = KafkaWriters.convertDataframe(
      spark.createDataset(List(input)).toDF(),
      None,
      Seq.empty,
      tm,
      None
    )
    assert(
      res
        .selectExpr("headers as _1", "cast (value as string) as _2")
        .as[(String, String)]
        .collect() sameElements List(input.copy(_2 = new String(input._2, StandardCharsets.UTF_8)))
    )
  }

  it should "convert in case of single binary topic with only value" in {
    val tm    = TopicModel.binary("name", 0L, 1, 1, None, None, None, TopicCompression.Disabled)
    val input = "test".getBytes(StandardCharsets.UTF_8)
    val res = KafkaWriters.convertDataframe(
      spark.createDataset(List(input)).toDF(),
      None,
      Seq.empty,
      tm,
      None
    )
    assert(
      res.selectExpr("cast (value as string)").as[String].collect() sameElements List(
        new String(input, StandardCharsets.UTF_8)
      )
    )
  }

  // avro single

  it should "convert in case of single avro topic with header and key" in {
    val schema = SchemaBuilder
      .builder("wasp")
      .record("test")
      .fields()
      .requiredString("name")
      .requiredString("surname")
      .requiredString("header")
      .requiredString("key")
      .endRecord()

    val tm = TopicModel.avro(
      "name",
      0L,
      1,
      1,
      Some("key"),
      Some("header"),
      false,
      schema,
      TopicCompression.Disabled,
      SubjectStrategy.None,
      None
    )
    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")
    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, tm, None)

    val collectedOutput =
      output.selectExpr("key as _1", "headers as _2", "value as _3").as[(String, String, Array[Byte])].collect().toList

    collectedOutput.map(_._2).foreach(x => assert(x === "h"))
    assert(collectedOutput.map(_._1).sorted == List("IT", "ES").sorted)
    collectedOutput.map(_._3).foreach { bytes =>
      val record: GenericRecord = parseAvro(schema, bytes)
      assert(Set("IT", "ES").contains(record.get("key").toString))
      assert(record.get("header").toString === "h")
      assert(Set("Antonio", "Mario").contains(record.get("name").toString))
      assert(Set("Ferrulli", "Murgia").contains(record.get("surname").toString))
    }
  }

  it should "convert in case of single avro topic with header and key but not in value" in {
    val schema = SchemaBuilder
      .builder("wasp")
      .record("test")
      .fields()
      .requiredString("name")
      .requiredString("surname")
      .endRecord()

    val tm = TopicModel
      .avro(
        "name",
        0L,
        1,
        1,
        Some("key"),
        Some("header"),
        false,
        schema,
        TopicCompression.Disabled,
        SubjectStrategy.None,
        None
      )
      .copy(valueFieldsNames = Some(List("name", "surname")))
    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")
    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, tm, None)

    val collectedOutput =
      output.selectExpr("key as _1", "headers as _2", "value as _3").as[(String, String, Array[Byte])].collect().toList

    collectedOutput.map(_._2).foreach(x => assert(x === "h"))
    assert(collectedOutput.map(_._1).sorted == List("IT", "ES").sorted)
    collectedOutput.map(_._3).foreach { bytes =>
      val record: GenericRecord = parseAvro(schema, bytes)
      assert(Set("Antonio", "Mario").contains(record.get("name").toString))
      assert(Set("Ferrulli", "Murgia").contains(record.get("surname").toString))
    }
  }

  it should "convert in case of single avro topic without header and key and more than values" in {
    val schema = SchemaBuilder
      .builder("wasp")
      .record("test")
      .fields()
      .requiredString("name")
      .requiredString("surname")
      .endRecord()

    val tm = TopicModel
      .avro(
        "name",
        0L,
        1,
        1,
        None,
        None,
        false,
        schema,
        TopicCompression.Disabled,
        SubjectStrategy.None,
        None
      )
      .copy(valueFieldsNames = Some(List("name", "surname")))
    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")
    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, tm, None)

    val collectedOutput =
      output.as[Array[Byte]].collect().toList

    collectedOutput.foreach { bytes =>
      val record: GenericRecord = parseAvro(schema, bytes)
      assert(Set("Antonio", "Mario").contains(record.get("name").toString))
      assert(Set("Ferrulli", "Murgia").contains(record.get("surname").toString))
    }
  }

  it should "throw an exception in case of single avro topic without specifying the schema" in {

    val tm = TopicModel(
      name = "name",
      creationTime = 0L,
      partitions = 1,
      replicas = 1,
      topicDataType = "avro",
      keyFieldName = None,
      headersFieldName = None,
      valueFieldsNames = Some(List("name", "surname")),
      useAvroSchemaManager = false,
      schema = BsonDocument(),
      topicCompression = TopicCompression.Disabled,
      subjectStrategy = SubjectStrategy.None,
      keySchema = None
    )

    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")
    val caught = intercept[IllegalArgumentException] {
      KafkaWriters.convertDataframe(input, None, Seq.empty, tm, None)
    }
    assert(caught.getMessage === "Topic name datatype is avro therefore the schema should be mandatory")
  }

  // json single

  it should "convert in case of single json topic with header and key" in {

    val tm = TopicModel.json(
      "name",
      0L,
      1,
      1,
      Some("key"),
      Some("header"),
      TopicCompression.Disabled
    )
    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")
    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, tm, None)

    val collectedOutput =
      output.selectExpr("key as _1", "headers as _2", "value as _3").as[(String, String, Array[Byte])].collect().toList

    collectedOutput.map(_._2).foreach(x => assert(x === "h"))
    assert(collectedOutput.map(_._1).sorted == List("IT", "ES").sorted)
    val expected = List(
      JsObject(
      "name" -> JsString("Antonio"),
      "surname" -> JsString("Murgia"),
      "header" -> JsString("h"),
      "key" -> JsString("IT")),
      JsObject(
        "name" -> JsString("Mario"),
        "surname" -> JsString("Ferrulli"),
        "header" -> JsString("h"),
        "key" -> JsString("ES"))
    )
    collectedOutput.map(_._3).foreach { bytes =>
      assert(expected.contains(parseJson(bytes)))
    }
  }

  it should "convert in case of single json topic with header and key but not in value" in {
    val schema = SchemaBuilder
      .builder("wasp")
      .record("test")
      .fields()
      .requiredString("name")
      .requiredString("surname")
      .endRecord()

    val jsonTopicModel = TopicModel.json(
      "name",
      0L,
      1,
      1,
      Some("key"),
      Some("headers"),
      schema,
      TopicCompression.Disabled,
      None
    )

    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "headers", "key")
    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, jsonTopicModel, None)

    val collectedOutput =
      output.selectExpr("key as _1", "headers as _2", "value as _3").as[(String, String, Array[Byte])].collect().toList

    collectedOutput.map(_._2).foreach(x => assert(x === "h"))
    assert(collectedOutput.map(_._1).sorted == List("IT", "ES").sorted)
    val expected = List(
      JsObject(
        "name" -> JsString("Antonio"),
        "surname" -> JsString("Murgia")),
      JsObject(
        "name" -> JsString("Mario"),
        "surname" -> JsString("Ferrulli"))
    )
    collectedOutput.map(_._3).foreach { bytes =>
      assert(expected.contains(parseJson(bytes)))
    }
  }

  it should "convert in case of single json topic without header and key and more than values" in {
    val schema = SchemaBuilder
      .builder("wasp")
      .record("test")
      .fields()
      .requiredString("name")
      .requiredString("surname")
      .endRecord()

    val jsonTopicModel = TopicModel.json(
      "name",
      0L,
      1,
      1,
      None,
      None,
      schema,
      TopicCompression.Disabled,
      None
    ).copy(valueFieldsNames = Some(List("name", "surname")))
    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")
    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, jsonTopicModel, None)

    val collectedOutput =
      output.as[Array[Byte]].collect().toList

    val expected = List(
      JsObject(
        "name" -> JsString("Antonio"),
        "surname" -> JsString("Murgia")),
      JsObject(
        "name" -> JsString("Mario"),
        "surname" -> JsString("Ferrulli"))
    )
    collectedOutput.foreach { bytes =>
      assert(expected.contains(parseJson(bytes)))
    }
  }

  it should "convert in case of single json topic without specifying the schema" in {

    val jsonTopicModel = TopicModel.json(
      "name",
      0L,
      1,
      1,
      Some("key"),
      Some("header"),
      TopicCompression.Disabled
    )

    val input = spark
      .createDataset(
        List(
          ("Antonio", "Murgia", "h", "IT"),
          ("Mario", "Ferrulli", "h", "ES")
        )
      )
      .toDF("name", "surname", "header", "key")

    val output = KafkaWriters.convertDataframe(input, None, Seq.empty, jsonTopicModel, None)

    val collectedOutput =
      output.selectExpr("key as _1", "headers as _2", "value as _3").as[(String, String, Array[Byte])].collect().toList

    collectedOutput.map(_._2).foreach(x => assert(x === "h"))
    assert(collectedOutput.map(_._1).sorted == List("IT", "ES").sorted)
    val expected = List(
      JsObject(
        "name" -> JsString("Antonio"),
        "surname" -> JsString("Murgia"),
        "header" -> JsString("h"),
        "key" -> JsString("IT")),
      JsObject(
        "name" -> JsString("Mario"),
        "surname" -> JsString("Ferrulli"),
        "header" -> JsString("h"),
        "key" -> JsString("ES"))
    )
    collectedOutput.map(_._3).foreach { bytes =>
      assert(expected.contains(parseJson(bytes)))
    }
  }

  private def parseAvro(schema: Schema, bytes: Array[Byte]) = {
    val avroValue             = new SeekableByteArrayInput(bytes)
    val avroReader            = new GenericDatumReader[GenericRecord](schema)
    val decoder               = DecoderFactory.get.binaryDecoder(avroValue, null)
    val record: GenericRecord = avroReader.read(null, decoder)
    record
  }

  private def parseJson(bytes: Array[Byte]): JsValue = {
    import spray.json._
    new String(bytes, StandardCharsets.UTF_8).parseJson
  }
}

trait ConvertDataframeTestData { self: SparkSuite =>

  import spark.implicits._

  val schemaRight = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("surname", StringType, true)
    )
  )

  val schemaLeft = StructType(
    Array(
      StructField("error", StringType, true)
    )
  )

  val topicRight = TopicModel(
    name = "TopicRight",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("id"),
    headersFieldName = Some("headers"),
    valueFieldsNames = Some(Seq("right.name", "right.surname")),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaRight, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = None
  )

  val topicLeft = TopicModel(
    name = "TopicLeft",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("id"),
    headersFieldName = Some("headers"),
    valueFieldsNames = Some(Seq("left.error")),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaLeft, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = None
  )

  val dfForNestedValues = Seq(
    Message("A", None, Some(RightPart("john", "travolta")), "someHeaders", "TopicRight"),
    Message("B", Some(LeftPart("someErrorMessage")), None, "someHeaders", "TopicLeft")
  ).toDS()

  val testDfTopicModelWithSameSchema = Seq(
    Person(1, "mario", "rossi", "TOPIC-A"),
    Person(0, "luca", "bobo", "TOPIC-B")
  ).toDS

  val testDfTopicModelWithSameSchemaStringKey = Seq(
    ("1", "mario", "rossi", "TOPIC-A"),
    ("0", "luca", "bobo", "TOPIC-B")
  ).toDF("id", "name", "surname", "topic")

  val testDfTopicModelWithDifferentSchema = Seq(
    (Some("hey"), "pippo", "franco", "TOPIC-A", None, None),
    (None, "mickey", "mouse", "TOPIC-B", Some("AA23"), Some("ErrorMsg")),
    (Some("buh"), "pluto", "franco", "TOPIC-A", None, None),
    (None, "donald", "duck", "TOPIC-B", Some("AA44"), Some("ErrorMsg"))
  ).toDF("id", "name", "surname", "topic", "key", "error")

  val testDfTopicModelWithDifferentSchemaWithHeaders = Seq(
    (Some("hey"), "pippo", "franco", "TOPIC-A", Some("headerValue"), None, None),
    (None, "mickey", "mouse", "TOPIC-B", None, Some("AA23"), Some("ErrorMsg")),
    (Some("buh"), "pluto", "franco", "TOPIC-A", Some("headerValue"), None, None),
    (None, "donald", "duck", "TOPIC-B", None, None, Some("ErrorMsg"))
  ).toDF("id", "name", "surname", "topic", "headers", "key", "error")

  val schemaAddress = StructType(
    Array(
      StructField("address", StringType, true),
      StructField("headers", StringType, true)
    )
  )

  val schemaPerson = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("surname", StringType, true)
    )
  )

  val schemaErrorPerson = StructType(
    Array(
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("error", StringType, true)
    )
  )

  val schemaErrorForBinary = StructType(
    Array(
      StructField("error", BinaryType, true)
    )
  )

  val testTopicModelAddress_avro_keyKeySchemaValueHeaders = TopicModel(
    name = "address-topic",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("id"),
    headersFieldName = Some("headers"),
    valueFieldsNames = Some(schemaAddress.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaAddress, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = Some(SchemaBuilder.builder().stringBuilder().endString().toString)
  )

  val testTopicModelPerson_json_keyValueHeaders = TopicModel(
    name = "test",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = Some("id"),
    headersFieldName = Some("headers"),
    valueFieldsNames = Some(Seq("name", "surname")),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaPerson, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = None
  )

  val testTopicModelPerson_json_key_1 = TopicModel(
    name = "TOPIC-A",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = Some("id"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaPerson, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = None
  )

  val testTopicModelPerson_json_key_2 = TopicModel(
    name = "TOPIC-A",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = Some("id"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaPerson, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = None
  )

  val testTopicModelErrorPerson_json_key = TopicModel(
    name = "TOPIC-A",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "json",
    keyFieldName = Some("key"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaErrorPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters
        .convertStructToAvro(
          schemaErrorPerson,
          SchemaBuilder.builder().record("schema"),
          "wasp"
        )
        .toString
    ),
    keySchema = None
  )

  val testTopicModelPerson_avro_keyKeySchema_1 = TopicModel(
    name = "TOPIC-A",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("id"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaPerson, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = Some(SchemaBuilder.builder().intBuilder().endInt().toString)
  )

  val testTopicModelPerson_avro_key = TopicModel(
    name = "multiTopicWrite11",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("name"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters.convertStructToAvro(schemaPerson, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = None
  )

  val testTopicModelPerson_avro_keyKeySchema_2 = TopicModel(
    name = "testTopic2A",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("id"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument.apply(
      AvroSchemaConverters.convertStructToAvro(schemaPerson, SchemaBuilder.builder().record("schema"), "wasp").toString
    ),
    keySchema = Some(SchemaBuilder.builder().stringBuilder().endString().toString)
  )

  val testTopicModelErrorPerson_avro_key = TopicModel(
    name = "testTopic2B",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = Some("key"),
    headersFieldName = None,
    valueFieldsNames = Some(schemaErrorPerson.fieldNames.toList),
    useAvroSchemaManager = false,
    schema = BsonDocument.apply(
      AvroSchemaConverters
        .convertStructToAvro(schemaErrorPerson, SchemaBuilder.builder().record("schema"), "wasp")
        .toString
    ),
    keySchema = None
  )

  val testTopicModelError_binary = TopicModel(
    name = "blabla",
    creationTime = System.currentTimeMillis,
    partitions = 1,
    replicas = 1,
    topicDataType = "binary",
    keyFieldName = Some("key"),
    headersFieldName = None,
    valueFieldsNames = Some(Seq("error")),
    useAvroSchemaManager = false,
    schema = BsonDocument(
      AvroSchemaConverters
        .convertStructToAvro(schemaErrorForBinary, SchemaBuilder.builder().record("schema"), "wasp")
        .toString
    ),
    keySchema = None
  )

}

case class Person(id: Int, name: String, surname: String, topic: String)

case class KafkaOutput(key: Array[Byte], topic: String, value: String)

case class LeftPart(error: String)

case class RightPart(name: String, surname: String)

case class Message(id: String, left: Option[LeftPart], right: Option[RightPart], headers: String, topic: String)
