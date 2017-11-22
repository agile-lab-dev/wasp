package it.agilelab.bigdata.wasp.consumers.spark.metadata

import org.apache.spark.sql.Row

object Path{
  def apply(r: Row): Path = {
    Path(r.getString(0), r.getLong(1))
  }
}

case class Path(name: String, ts: Long)
case class Metadata(id: String,
                    sourceId: String,
                    arrivalTimestamp: Long,
                    lastSeenTimestamp: Long,
                    path: Array[Path])
