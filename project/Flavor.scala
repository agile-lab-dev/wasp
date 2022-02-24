sealed trait Flavor {
  val id: String
  val postfix: Option[String]
  val settings: Settings
  val dependencies: Dependencies
  val scalaVersion: ScalaVersion
}

object Flavor {
  case object CDH6 extends Flavor {
    private val versions                    = new Cdh6Versions()
    val postfix: Option[String]             = Some("cdh6")
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, scalaVersion)
    override val dependencies: Dependencies = new Cdh6Dependencies(versions)
    override val id: String                 = "CDH6"
  }
  case object Vanilla2 extends Flavor {
    private val versions                    = new Vanilla2Versions()
    val postfix: Option[String]             = None
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, scalaVersion)
    override val dependencies: Dependencies = new Vanilla2Dependencies(versions)
    override val id: String                 = "VANILLA2"
  }
  case object Vanilla2_2_12 extends Flavor {
    private val versions                    = new Vanilla2_2_12Versions()
    val postfix: Option[String]             = None
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, scalaVersion)
    override val dependencies: Dependencies = new Vanilla2Dependencies(versions)
    override val id: String                 = "VANILLA2_2_12"
  }

  case object CDP717 extends Flavor {
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override lazy val settings: Settings =
      new BasicSettings(
        new CDP717Resolvers(new BasicResolvers()),
        versions.jdk,
        scalaVersion,
        dependencies.parcelDependencies,
        dependencies.globalExclusions
      )
    override lazy val dependencies: CDP717Dependencies = new CDP717Dependencies(versions)
    lazy val postfix: Option[String]                   = Some("cdp717")
    private lazy val versions                          = new CDP717Versions()
    override val id: String                            = "CDP717"
  }

  case object EMR212 extends Flavor {
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override lazy val settings: Settings =
      new BasicSettings(new BasicResolvers(), versions.jdk, scalaVersion ,dependencies.overrides, dependencies.removeShims)
    override lazy val dependencies: EMR212Dependencies = new EMR212Dependencies(versions)
    lazy val postfix: Option[String] = Some("emr212")
    private lazy val versions        = new EMR212Versions()
    override val id: String = "EMR_2_12"
  }

  val DEFAULT: Flavor = Vanilla2

  def parse(s: String): Either[String, Flavor] = {
    s.toUpperCase match {
      case "CDH6" => Right(CDH6)
      case "VANILLA2" => Right(Vanilla2)
      case "VANILLA2_2_12" => Right(Vanilla2_2_12)
      case "CDP717"   => Right(CDP717)
      case "EMR_2_12" => Right(EMR212)
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

case class ScalaVersion(major: Int, minor: Int, revision: Int) {
  val raw: String = s"$major.$minor.$revision"
  def isMajorMinor(maj: Int, min: Int): Boolean = maj == major && min == minor
}
object ScalaVersion {
  def parseScalaVersion(s: String): ScalaVersion =
    s.split("\\.", 3) match {
      case Array(maj, min, rev) => ScalaVersion(maj.toInt, min.toInt, rev.toInt)
      case other                => throw new RuntimeException(s"Cannot parse $other as a version")
    }
}