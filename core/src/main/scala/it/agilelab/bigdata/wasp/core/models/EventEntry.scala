package it.agilelab.bigdata.wasp.core.models

import java.time.Instant


case class EventEntry(eventType: String,
                      eventId: String,
                      severity: String,
                      payload: String,
                      timestamp: Instant,
                      source:String,
                      sourceId: String,
                      eventRuleName: String)

case class Events(found: Long, entries: Seq[EventEntry])

