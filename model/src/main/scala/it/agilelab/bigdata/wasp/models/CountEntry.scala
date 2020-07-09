package it.agilelab.bigdata.wasp.models

import java.time.Instant

case class CountEntry(timestamp: Instant, count: Map[String, Int])
