package it.agilelab.bigdata.wasp.models.builder

sealed abstract class KVType(val name: String)

object KVType {

  final case object INT extends KVType("int")

  final case object LONG extends KVType("long")

  final case object STRING extends KVType("string")

  final case object BOOL extends KVType("boolean")

  final case object BYTE extends KVType("byte")

  final case object BYTES extends KVType("bytes")

  final case object DOUBLE extends KVType("double")

  final case object FLOAT extends KVType("float")

  final case object SHORT extends KVType("short")

  def get(s: String): Option[KVType] = s.toLowerCase match {
    case "int" | "integer" | "scala.int" => Some(INT)
    case "long" | "scala.long" => Some(LONG)
    case "string" | "java.lang.string" => Some(STRING)
    case "boolean" | "scala.boolean" | "bool" => Some(BOOL)
    case "byte" | "scala.byte" => Some(BYTE)
    case "bytes" => Some(BYTES)
    case "double" | "scala.double" => Some(DOUBLE)
    case "float" | "scala.float" => Some(FLOAT)
    case "short" | "scala.short" => Some(SHORT)
    case _ => None
  }


}
