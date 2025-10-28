sealed trait Flavor {
  val id: String
  val postfix: Option[String]
  val settings: Settings
  val dependencies: Dependencies
  val scalaVersion: ScalaVersion
}

object Flavor {

  case object Spark3_5_Emr770 extends Spark_3 {
    override val id: String                            = "SPARK3.5-EMR770"
    override lazy val dependencies: Spark3Dependencies = Spark35Emr770Dependencies
    override val postfix: Option[String]               = Some("3_5_emr770")

  }

  case object Spark3_5 extends Spark_3 {
    override val id: String                            = "SPARK3.5"
    override lazy val dependencies: Spark3Dependencies = Spark35Dependencies
    override val postfix: Option[String]               = Some("3_5")

  }

  case object Spark3_4 extends Spark_3 {
    override val id: String                            = "SPARK3.4"
    override lazy val dependencies: Spark3Dependencies = Spark34Dependencies
    override val postfix: Option[String]               = None
  }

  case object Spark3_3 extends Spark_3 {
    override val id: String                            = "SPARK3.3"
    override lazy val dependencies: Spark3Dependencies = Spark33Dependencies
    override val postfix: Option[String]               = Some("3_3")
  }

  trait Spark_3 extends Flavor {
    val dependencies: Spark3Dependencies
    lazy val versions: Spark3Versions            = dependencies.versions
    override lazy val scalaVersion: ScalaVersion = ScalaVersion.parseScalaVersion(versions.scala)
    override lazy val settings: Settings = new BasicSettings(
      resolver = new BasicResolvers(),
      jdkVersionValue = versions.jdk,
      scalaVersionValue = scalaVersion,
      // this is needed because otherwise parallel-write-plugin,
      // only during tests with coverage enabled will wrongly put
      // in the classpath version 3.3.5 (that we can't understand where it
      // comes from, since it's nowhere to be found in the dependencyTree).
      // the exception thrown is:
      //   java.lang.NoSuchMethodError: 'org.apache.hadoop.fs.FSBuilder org.apache.hadoop.fs.FutureDataInputStreamBuilder.opt(java.lang.String, long)
      overrideDep = Seq(dependencies.hadoopClientApi)
    )
  }

  val DEFAULT: Flavor = Spark3_4

  def parse(s: String): Either[String, Flavor] = {
    s.toUpperCase match {
      case "SPARK3.5-EMR770" => Right(Spark3_5_Emr770)
      case "SPARK3.5" => Right(Spark3_5)
      case "SPARK3.4" => Right(Spark3_4)
      case "SPARK3.3" => Right(Spark3_3)
      case _          => Left(s"Cannot parse flavor [${s}]")
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
  val raw: String                               = s"$major.$minor.$revision"
  def isMajorMinor(maj: Int, min: Int): Boolean = maj == major && min == minor
}
object ScalaVersion {
  def parseScalaVersion(s: String): ScalaVersion =
    s.split("\\.", 3) match {
      case Array(maj, min, rev) => ScalaVersion(maj.toInt, min.toInt, rev.toInt)
      case other                => throw new RuntimeException(s"Cannot parse $other as a version")
    }
}
