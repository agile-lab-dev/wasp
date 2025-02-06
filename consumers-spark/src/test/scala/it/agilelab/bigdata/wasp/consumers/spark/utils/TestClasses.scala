package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.util.Utf8

import java.io.{ByteArrayOutputStream, EOFException}
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class NestedCaseClass(d: Double, l: Long, s: String)

case class UglyCaseClass(
                          a: Array[Byte],
                          b: Array[Int],
                          na: Array[NestedCaseClass],
                          d: Date,
                          ts: Timestamp,
                          n: NestedCaseClass,
                          sm: Map[String, Int],
                          som: Map[String, Option[Double]],
                          mm: Map[String, Map[String, Option[Double]]],
                          m: Map[String, NestedCaseClass]
                        ) {
  override def equals(obj: Any): Boolean = obj match {
    case other: UglyCaseClass =>
      this.a.sameElements(other.a) &&
        this.b.sameElements(other.b) &&
        this.na.sameElements(other.na) &&
        this.d == other.d &&
        this.ts == other.ts &&
        this.n == other.n &&
        this.sm == other.sm &&
        this.som == other.som &&
        this.mm == other.mm &&
        this.m == other.m
  }
}

object TestClasses {
  val parser = new Schema.Parser()

  val uglySchema: Schema = parser.parse(
    """
{
  "type": "record",
  "name": "UglyCaseClass",
  "fields": [
    {"name": "a", "type": "bytes"},
    {"name": "b", "type": {"type": "array", "items": "int"}},
    {"name": "na", "type": {"type": "array", "items": {"type": "record","name": "NestedCaseClass","fields": [{"name": "d", "type": "double"},{"name": "l", "type": "long"},{"name": "s", "type": "string"}]}}},
    {"name": "d", "type": "long"},
    {"name": "ts", "type": "long"},
    {"name": "n", "type": "NestedCaseClass"},
    {"name": "sm", "type": {"type": "map", "values": "int"}},
    {"name": "som","type": {"type": "map","values": ["null","double"]}},
    {"name": "mm","type": {"type": "map","values": {"type": "map","values": ["null","double"]}}},
    {"name": "m", "type": {"type": "map", "values": "NestedCaseClass"}}
  ]
}
"""
  )

  // Helper method to convert `NestedCaseClass` to `GenericRecord`
  def encodeNestedCaseClass(nested: NestedCaseClass): GenericRecord = {
    val record = new GenericData.Record(uglySchema.getField("n").schema())
    record.put("d", nested.d)
    record.put("l", nested.l)
    record.put("s", nested.s)
    record
  }

  def encodeUglyCaseClass(ugly: UglyCaseClass): GenericRecord = {
    val record = new GenericData.Record(uglySchema)

    record.put("a", ByteBuffer.wrap(ugly.a))
    record.put("b", ugly.b.toList.asJava)
    record.put("na", ugly.na.map(encodeNestedCaseClass).toList.asJava)
    record.put("d", ugly.d.getTime)   // Convert Date to long
    record.put("ts", ugly.ts.getTime) // Convert Timestamp to long
    record.put("n", encodeNestedCaseClass(ugly.n))
    record.put("sm", ugly.sm.asJava)
    record.put(
      "som",
      ugly.som.mapValues(o => o.map(java.lang.Double.valueOf).orNull).asJava
    )
    record.put(
      "mm",
      ugly.mm
        .mapValues(
          _.mapValues(o => o.map(java.lang.Double.valueOf).orNull).asJava
        )
        .asJava
    )
    record.put("m", ugly.m.mapValues(encodeNestedCaseClass).asJava)

    record
  }

  def decodeNestedCaseClass(record: GenericRecord): NestedCaseClass = {
    NestedCaseClass(
      record.get("d").asInstanceOf[Double],
      record.get("l").asInstanceOf[Long],
      record.get("s").asInstanceOf[Utf8].toString
    )
  }

  def decodeUglyCaseClass(record: GenericRecord): UglyCaseClass = {
    UglyCaseClass(
      record.get("a").asInstanceOf[ByteBuffer].array(),
      record.get("b").asInstanceOf[GenericData.Array[Int]].asScala.toArray,
      record
        .get("na")
        .asInstanceOf[GenericData.Array[GenericRecord]]
        .asScala
        .map(decodeNestedCaseClass)
        .toArray,
      new Date(record.get("d").asInstanceOf[Long]),
      new Timestamp(record.get("ts").asInstanceOf[Long]),
      decodeNestedCaseClass(record.get("n").asInstanceOf[GenericRecord]),
      record
        .get("sm")
        .asInstanceOf[java.util.Map[Utf8, Int]]
        .asScala
        .map { case (k, v) => k.toString -> v }
        .toMap,
      record
        .get("som")
        .asInstanceOf[java.util.Map[Utf8, java.lang.Double]]
        .asScala
        .map {
          case (k, v) =>
            k.toString -> Option(v).map(_.toDouble)
        }
        .toMap,
      record
        .get("mm")
        .asInstanceOf[
          java.util.Map[Utf8, java.util.Map[Utf8, java.lang.Double]]
        ]
        .asScala
        .map {
          case (k, values: java.util.Map[Utf8, java.lang.Double]) =>
            k.toString -> values.asScala.map {
              case (k, v) =>
                k.toString -> Option(v).map(_.toDouble)
            }.toMap
        }
        .toMap,
      record
        .get("m")
        .asInstanceOf[java.util.Map[Utf8, GenericRecord]]
        .asScala
        .map { case (k, v) => k.toString -> decodeNestedCaseClass(v) }
        .toMap
    )
  }

  val nested1 = NestedCaseClass(1.23, 123L, "abc")
  val nested2 = NestedCaseClass(4.56, 456L, "def")

  val ugly = UglyCaseClass(
    a = Array[Byte](1, 2, 3),
    b = Array(1, 2, 3),
    na = Array(nested1, nested2),
    d = Date.valueOf("2025-02-04"),
    ts = Timestamp.valueOf("2025-02-04 12:00:00"),
    n = nested1,
    sm = Map("one"   -> 1, "two" -> 2),
    som = Map("key1" -> Some(1.1), "key2" -> None),
    mm = Map("outer" -> Map("inner" -> Some(2.2))),
    m = Map("nested" -> nested2)
  )

  def serializeToBytes(record: GenericRecord, schema: Schema): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val datumWriter: DatumWriter[GenericRecord] =
      new GenericDatumWriter[GenericRecord](schema)
    val encoder: BinaryEncoder =
      EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
    datumWriter.write(record, encoder)
    encoder.flush()
    byteArrayOutputStream.toByteArray
  }

  def deserializeFromBytes(
                            bytes: Array[Byte],
                            schema: Schema
                          ): List[GenericRecord] = {
    val byteArrayInputStream = new java.io.ByteArrayInputStream(bytes)
    val datumReader: DatumReader[GenericRecord] =
      new GenericDatumReader[GenericRecord](schema)
    val decoder: BinaryDecoder =
      DecoderFactory.get().binaryDecoder(byteArrayInputStream, null)

    val result = new ListBuffer[GenericRecord]
    result += datumReader.read(null, decoder) // we don't want to catch this, at least one element should be there
    try {
      do {
        result += datumReader.read(null, decoder)
      } while (result.last != null) // this check is in constant time
    } catch {
      case _: EOFException => // we catch only EOFExc, only after the first element
    }
    result.toList
  }
}
