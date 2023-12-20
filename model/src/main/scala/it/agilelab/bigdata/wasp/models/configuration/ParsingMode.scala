package it.agilelab.bigdata.wasp.models.configuration

sealed abstract class ParsingMode(val mode: String)

case object Strict extends ParsingMode("strict")

case object Ignore extends ParsingMode("ignore")

case object Handle extends ParsingMode("handle")

object ParsingMode {
  val map = List(Strict, Ignore, Handle).map(x => (x.mode, x)).toMap

  def asString(parsingMode: ParsingMode): String = parsingMode.mode

  def fromString(parsingMode: String): Option[ParsingMode] = map.get(parsingMode)
}
