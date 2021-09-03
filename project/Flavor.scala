sealed trait Flavor {
  val postfix: Option[String]
  val settings: Settings
  val dependencies: Dependencies
}

object Flavor {
  case object CDH6 extends Flavor {
    private val versions                    = new Cdh6Versions()
    val postfix: Option[String]             = Some("cdh6")
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, versions.scala)
    override val dependencies: Dependencies = new Cdh6Dependencies(versions)
  }
  case object Vanilla2 extends Flavor {
    private val versions                    = new Vanilla2Versions()
    val postfix: Option[String]             = None
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, versions.scala)
    override val dependencies: Dependencies = new Vanilla2Dependencies(versions)
  }
  val DEFAULT: Flavor = Vanilla2

  def parse(s: String): Either[String, Flavor] = {
    s.toUpperCase match {
      case "CDH6" => Right(CDH6)
      case "VANILLA2" => Right(Vanilla2)
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
