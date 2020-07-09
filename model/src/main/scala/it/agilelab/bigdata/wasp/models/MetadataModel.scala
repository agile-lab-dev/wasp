package it.agilelab.bigdata.wasp.models

case class PathModel (
  val name: String,
  val ts: Long
)

case class MetadataModel(
  val id: String,
  val sourceId: String,
  val arrivalTimestamp: Long,
  val lastSeenTimestamp: Long,
  val path: Array[PathModel]
)

trait Metadata{
  val metadata: MetadataModel
}