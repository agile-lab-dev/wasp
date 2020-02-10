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


  val schema: Schema = AvroSchema[UglyCaseClass]
}


case class NestedCaseClass(d: Double, l: Long, s: String)

case class UglyCaseClass(a: Array[Byte],
                         b: Array[Int],
                         na: Array[NestedCaseClass],
                         d: Date,
                         ts: Timestamp,
                         n: NestedCaseClass,
                         sm: Map[String, Int],
                         som: Map[String, Option[Double]],
                         mm: Map[String, Map[String, Option[Double]]],
                         m: Map[String, NestedCaseClass])

object UglyCaseClass {

  import TestSchemas.implicits._

  /**
    * Move them here for 3 reasons:
    * 1. performance reason 1, having the macro regenerate them at every call site takes a lot of compile time
    * 2. performance reason 2, the macro **can** generate a Lazy[_] which is evaluated at runtime, therefore
    * having a different macro application at every call site, generates a different Lazy[_] instance at every
    * call site which generates a lot of overhead (this has been profiled by Marco Gaido in the good old days)
    * 3. (the real one) the incremental compiler doesn't cope very well with those macros and sometimes does not re-run
    * the macro when the case class has changed if the file where the macro application happens has not changed
    */
  implicit val schemaFor: SchemaFor[UglyCaseClass] = SchemaFor[UglyCaseClass]
  implicit val toRecord: ToRecord[UglyCaseClass] = ToRecord[UglyCaseClass]
  implicit val fromRecord: FromRecord[UglyCaseClass] = FromRecord[UglyCaseClass]
}