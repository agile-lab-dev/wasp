package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.sql.{Date, Timestamp}

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import org.apache.avro.Schema


object SchemaHolder {

/*
  lazy implicit val schemaForDate : SchemaFor[Date] = new SchemaFor[Date] {
    override def apply(): Schema = Schema.create(Schema.Type.LONG)
  }

  lazy implicit val schemaForTimestamp : SchemaFor[Timestamp] = new SchemaFor[Timestamp] {
    override def apply(): Schema = Schema.create(Schema.Type.LONG)
  }

  lazy val schema : Schema = AvroSchema[UglyCaseClass]



  def main(args: Array[String]): Unit = {

    println(schema.toString(true))
  }
  */



  val jsonSchema = """{
                     |  "type" : "record",
                     |  "name" : "UglyCaseClass",
                     |  "namespace" : "it.agilelab.bigdata.wasp.consumers.spark.utils",
                     |  "fields" : [ {
                     |    "name" : "a",
                     |    "type" : "bytes"
                     |  }, {
                     |    "name" : "b",
                     |    "type" : {
                     |      "type" : "array",
                     |      "items" : "int"
                     |    }
                     |  }, {
                     |    "name" : "na",
                     |    "type" : {
                     |      "type" : "array",
                     |      "items" : {
                     |        "type" : "record",
                     |        "name" : "NestedCaseClass",
                     |        "fields" : [ {
                     |          "name" : "d",
                     |          "type" : "double"
                     |        }, {
                     |          "name" : "l",
                     |          "type" : "long"
                     |        }, {
                     |          "name" : "s",
                     |          "type" : "string"
                     |        } ]
                     |      }
                     |    }
                     |  }, {
                     |    "name" : "d",
                     |    "type" : "long"
                     |  }, {
                     |    "name" : "ts",
                     |    "type" : "long"
                     |  }, {
                     |    "name" : "n",
                     |    "type" : "NestedCaseClass"
                     |  } ]
                     |}""".stripMargin
}


case class NestedCaseClass(d: Double, l: Long, s: String)

case class UglyCaseClass(a: Array[Byte],
                         b : Array[Int],
                         na : Array[NestedCaseClass],
                         d: Date,
                         ts: Timestamp,
                         n: NestedCaseClass
                        )
