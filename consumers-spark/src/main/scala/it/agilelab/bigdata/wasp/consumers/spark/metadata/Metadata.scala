package it.agilelab.bigdata.wasp.consumers.spark.metadata

case class Path(name: String, ts: Long)
case class Metadata(id: String,
                    sourceId: String,
                    arrivalTimestamp: Long,
                    lastSeenTimestamp: Long,
                    path: Array[Path])
