package it.agilelab.bigdata.wasp.core.eventengine

import org.apache.avro.Schema

//This class may not be required, but it is useful for testing purpose
case class Event(
                  eventId: String,
                  eventType: String,
                  severity: String,
                  payload: Option[String], //TODO
                  timestamp: Long,
                  source: String, // Name of the data source => name of the DataFrame or table
                  sourceId: Option[String], // Optional id of the message which ignited the event
                  eventRuleName: String) {
}
object Event {
  val SCHEMA: Schema = {
    val resource = getClass.getClassLoader.getResourceAsStream("it_agilelab_bigdata_wasp_core_eventengine_event.avsc")
    try {
      new Schema.Parser().parse(resource)
    } finally {
      resource.close()
    }
  }
}