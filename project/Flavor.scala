sealed trait Flavor {
  val id: String
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
    override val id: String = "CDH6"
  }
  case object Vanilla2 extends Flavor {
    private val versions                    = new Vanilla2Versions()
    val postfix: Option[String]             = None
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, versions.scala)
    override val dependencies: Dependencies = new Vanilla2Dependencies(versions)
    override val id: String = "VANILLA2"
  }
  case object Vanilla2_2_12 extends Flavor {
    private val versions                    = new Vanilla2_2_12Versions()
    val postfix: Option[String]             = None
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, versions.scala)
    override val dependencies: Dependencies = new Vanilla2Dependencies(versions)
    override val id: String = "VANILLA2_2_12"
  }

  case object CDP717 extends Flavor {
    override lazy val settings: Settings =
      new BasicSettings(new CDP717Resolvers(new BasicResolvers()), versions.jdk, versions.scala, dependencies.parcelDependencies, dependencies.globalExclusions)
    override lazy val dependencies: CDP717Dependencies = new CDP717Dependencies(versions)
    lazy val postfix: Option[String] = Some("cdp717")
    private lazy val versions        = new CDP717Versions()
    override val id: String = "CDP717"

  }

  val DEFAULT: Flavor = Vanilla2

  def parse(s: String): Either[String, Flavor] = {
    s.toUpperCase match {
      case "CDH6" => Right(CDH6)
      case "VANILLA2" => Right(Vanilla2)
      case "VANILLA2_2_12" => Right(Vanilla2_2_12)
      case "CDP717"   => Right(CDP717)
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
