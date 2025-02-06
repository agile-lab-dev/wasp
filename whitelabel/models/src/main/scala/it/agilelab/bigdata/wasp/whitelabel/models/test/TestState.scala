package it.agilelab.bigdata.wasp.whitelabel.models.test

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{Schema, SchemaBuilder}

case class TestState(count: Int, list: List[TestNestedDocument], newValue: String)

object TestState{

  val nestedSchema: Schema = SchemaBuilder
    .record("TestNestedDocument")
    .fields()
    .name("field1")
    .`type`()
    .stringType()
    .noDefault()
    .name("field2")
    .`type`()
    .longType()
    .noDefault()
    .name("field3")
    .`type`()
    .optional()
    .stringType()
    .endRecord()

  val schema: Schema = SchemaBuilder
    .record("TestState")
    .fields()
    .name("count")
    .`type`()
    .intType()
    .noDefault()
    .name("list")
    .`type`()
    .array()
    .items(nestedSchema)
    .noDefault()
    .name("newValue")
    .`type`()
    .stringType()
    .noDefault()
    .endRecord();


  def toRecord(data: TestState): GenericRecord = {

    val nestedRecordList: Seq[GenericData.Record] = data.list.map { el => {
        val nestedRecordEl = new GenericData.Record(nestedSchema)
        nestedRecordEl.put("field1", el.field1)
        nestedRecordEl.put("field2", el.field2)
        nestedRecordEl.put("field3", el.field3)
        nestedRecordEl
      }
    }
    val record = new GenericData.Record(schema)
    record.put("count", data.count)
    record.put("list", nestedRecordList)
    record.put("newValue", data.newValue)

    record
  }

  def fromRecord(record: GenericRecord): TestState = {
    val count = record.get("count").asInstanceOf[java.lang.Integer]
    val list = record.get("list").asInstanceOf[List[GenericRecord]]
    val listOfTestNestedDocument = list.map(x => {
      TestNestedDocument(
        x.get("field1").toString,
        x.get("field2").asInstanceOf[java.lang.Long],
        Option(x.get("field3")).map(_.toString)
      )
    })
    val newValue =  record.get("newValue").toString
    TestState(count, listOfTestNestedDocument, newValue)
  }

}