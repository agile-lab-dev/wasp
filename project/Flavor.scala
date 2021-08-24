sealed trait Flavor {
  val postfix: Option[String]
}

object Flavor {
  case object CDH6 extends Flavor {
    val postfix: Option[String] = None
  }
  val DEFAULT: Flavor = CDH6

  def parse(s: String): Either[String, Flavor] = {
    s.toUpperCase match {
      case "CDH6" => Right(CDH6)
      case _      => Left(s"Cannot parse flavor [${s}]")
    }
  }

  def currentFlavor(): Flavor = {
    Utils.resolveVariable("WASP_FLAVOR").map(parse).getOrElse(Right(DEFAULT)) match {
      case Right(f)    => f
      case Left(error) => throw new RuntimeException(error)
    }
  }
}
