package it.agilelab.bigdata.wasp.models

case class Counts(logs: Seq[CountEntry], telemetry: Seq[CountEntry], events: Seq[CountEntry])
