package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.sql.{Date, Timestamp}

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field


object TestSchemas {

  object implicits {
    implicit object DateTimeToValue extends ToValue[Date] {
      override def apply(value: Date): Long = value.getTime
    }

    implicit object DateTimeFromValue extends FromValue[Date] {
      override def apply(value: Any, field: Field): Date = new Date(value.asInstanceOf[Long])
    }

    implicit object TimestampToValue extends ToValue[Timestamp] {
      override def apply(value: Timestamp): Long = value.getTime
    }

    implicit object TimestampFromValue extends FromValue[Timestamp] {
      override def apply(value: Any, field: Field): Timestamp = new Timestamp(value.asInstanceOf[Long])
    }

    implicit object SchemaForDate extends SchemaFor[Date] {
      override def apply(): Schema = Schema.create(Schema.Type.LONG)
    }

    implicit object SchemaForTimestamp extends SchemaFor[Timestamp] {
      override def apply(): Schema = Schema.create(Schema.Type.LONG)
    }

  }


  val schema: Schema = {
    import implicits.{SchemaForDate, SchemaForTimestamp}
    AvroSchema[UglyCaseClass]
  }
}


case class NestedCaseClass(d: Double, l: Long, s: String)

case class UglyCaseClass(a: Array[Byte],
                         b : Array[Int],
                         na : Array[NestedCaseClass],
                         d: Date,
                         ts: Timestamp,
                         n: NestedCaseClass)
