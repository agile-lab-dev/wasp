package it.agilelab.bigdata.wasp.core.eventengine

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
