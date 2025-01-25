sealed trait Flavor {
  val id: String
  val postfix: Option[String]
  val settings: Settings
  val dependencies: Dependencies
  val scalaVersion: ScalaVersion
}

object Flavor {

  case object Vanilla2_2_12 extends Flavor {
    private val versions                    = new Vanilla2Versions()
    val postfix: Option[String]             = None
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override val settings: Settings         = new BasicSettings(new BasicResolvers(), versions.jdk, scalaVersion)
    override val dependencies: Dependencies = new Vanilla2Dependencies(versions)
    override val id: String                 = "VANILLA2_2_12"
  }

  case object CDP719 extends Flavor {
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override lazy val settings: Settings =
      new BasicSettings(
        new CDP719Resolvers(new BasicResolvers()),
        versions.jdk,
        scalaVersion,
        dependencies.parcelDependencies,
        dependencies.globalExclusions
      )
    override lazy val dependencies: CDP719Dependencies = new CDP719Dependencies(versions)
    lazy val postfix: Option[String]                   = Some("cdp719")
    private lazy val versions                          = new CDP719Versions()
    override val id: String                            = "CDP719"
  }

  case object EMR613 extends Flavor {
    override val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override lazy val settings: Settings =
      new BasicSettings(new BasicResolvers(), versions.jdk, scalaVersion, dependencies.overrides, dependencies.removeShims)
    override lazy val dependencies: EMR613Dependencies = new EMR613Dependencies(versions)
    lazy val postfix: Option[String] = Some("emr613")
    private lazy val versions = new EMR613Versions()
    override val id: String = "EMR_6_13"
  }

  val DEFAULT: Flavor = Vanilla2_2_12

  def parse(s: String): Either[String, Flavor] = {
    s.toUpperCase match {
      case "VANILLA2_2_12" => Right(Vanilla2_2_12)
      case "CDP719"   => Right(CDP719)
      case "EMR_6_13" => Right(EMR613)
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
