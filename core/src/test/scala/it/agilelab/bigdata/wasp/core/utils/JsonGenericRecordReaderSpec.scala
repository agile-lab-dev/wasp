package it.agilelab.bigdata.wasp.core.utils

import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class JsonGenericRecordReaderSpec extends FlatSpec with Matchers {
  val schema = new Schema.Parser().parse(
    """{
      |    "type": "record",
      |    "name": "CommonSensorEvent",
      |    "namespace": "it.enel.tlc.data",
      |    "fields": [
      |        {
      |            "name": "ts",
      |            "type": "string",
      |            "comment": "Date when the event happened in UTC TZ and following format: yyyy-MM-dd'T'HH:mm:ss.SSS"
      |        },
      |        {
      |            "name": "ts_cc",
      |            "type": "string",
      |            "comment": "Date when the event was captured in UTC TZ and following format: yyyy-MM-dd'T'HH:mm:ss.SSS"
      |        },
      |        {
      |            "name": "note",
      |            "type": [
      |              "null",
      |              "string",
      |              {"type": "map", "values": "long"},
      |              "CommonSensorEvent",
      |              {"type": "array", "items": "string"},
      |              {"type": "record", "name": "Foo", "fields": [
      |                {
      |                  "name": "bar",
      |                  "type": {"type": "array", "items": "int"},
      |                  "comment": "Date when the event happened in UTC TZ and following format: yyyy-MM-dd'T'HH:mm:ss.SSS"
      |                }]
      |              }
      |            ],
      |            "comment": "nullable field",
      |            "default": null
      |        }
      |    ]
      |}
      |""".stripMargin
  )
  val case1 = """{
                |  "ts":"a",
                |  "ts_cc": "b",
                |  "note": {
                |    "abc": 3
                |  }
                |}
                |""".stripMargin.getBytes -> "{\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": {\"abc\": 3}}"
  val case2 = """{
                |  "ts":"a",
                |  "ts_cc": "b"
                |}
                |""".stripMargin.getBytes -> "{\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": null}"
  val case3 = """{
                |  "ts":"a",
                |  "ts_cc": "b",
                |  "note": ""
                |}
                |""".stripMargin.getBytes -> "{\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": \"\"}"
  val case4 = """{
                |  "ts":"a",
                |  "ts_cc": "b",
                |  "note": {
                |    "ts":"a",
                |    "ts_cc": "b",
                |    "note": ""
                |  }
                |}
                |""".stripMargin.getBytes -> "{\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": {\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": \"\"}}"
  val case5 = """{
                |  "ts":"a",
                |  "ts_cc": "b",
                |  "note": ["dio", "mio"]
                |}
                |""".stripMargin.getBytes -> "{\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": [\"dio\", \"mio\"]}"

  val case6 = """{
                |  "ts":"a",
                |  "ts_cc": "b",
                |  "note": {
                |    "bar": 3
                |  }
                |}
                |""".stripMargin.getBytes -> "{\"ts\": \"a\", \"ts_cc\": \"b\", \"note\": {\"bar\": 3}}"
  it should "convert a json to a generic record from a schema with a union field" in {
    val cases = case1 :: case2 :: case3 :: case4 :: case5 :: case6 :: Nil
    cases.foreach {
      case (data, result) =>
        val record = new JsonAvroConverter().convertToGenericDataRecord(data, schema)
        record.toString shouldBe result
    }
  }
}
