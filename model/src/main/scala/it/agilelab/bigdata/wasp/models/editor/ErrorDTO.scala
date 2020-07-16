package it.agilelab.bigdata.wasp.models.editor

case class ErrorDTO(msg: String)

object ErrorDTO {
  def alreadyExists(entity: String, value: String): ErrorDTO = ErrorDTO(s"$entity already exists: $value")
  def unknownArgument(entity: String, value: String): ErrorDTO = ErrorDTO(s"Unknown $entity type: $value")
}
