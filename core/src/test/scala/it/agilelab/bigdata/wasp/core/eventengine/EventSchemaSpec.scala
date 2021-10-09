package it.agilelab.bigdata.wasp.core.eventengine

import com.sksamuel.avro4s.AvroSchema
import org.scalatest.{FlatSpec, Matchers}

class EventSchemaSpec extends FlatSpec with Matchers {

  it should "keep the schema updated with the case class" in {
    AvroSchema[Event] shouldBe Event.SCHEMA
  }

}
