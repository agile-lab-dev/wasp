import com.jsuereth.sbtpgp.PgpKeys._
import xerial.sbt.Sonatype.SonatypeKeys._
import sbt.Keys._
import sbt.{ScalaVersion => _, _}
import sbtbuildinfo.BuildInfoKey
import sbtbuildinfo.BuildInfoKeys.{buildInfoKeys, buildInfoPackage}

/*
 * Common settings for all the modules.
 *
 * See project/Versions.scala for the versions definitions.
 */
trait Resolvers {
  val resolvers: Seq[MavenRepository]
}

trait Settings {
  val commonSettings: Seq[Def.Setting[_]]
  val sbtBuildInfoSettings: Seq[Def.Setting[_]]
  val disableParallelTests: Seq[Def.Setting[_]]
}

class CDP717Resolvers(other: Resolvers) extends Resolvers {

  override val resolvers: Seq[MavenRepository] = other.resolvers ++ Seq(
    "Cloudera runtime resolvers" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Agile lab snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Twitter" at "https://maven.twttr.com/"
  )
}

class BasicResolvers extends Resolvers {
  val mavenLocalRepo            = Resolver.mavenLocal
  val sonatypeReleaseRepos       = Resolver.sonatypeOssRepos("releases")
  val sonatypeSnapshotsRepos     = Resolver.sonatypeOssRepos("snapshots")
  val restletMavenRepo          = "Restlet Maven repository" at "https://maven.restlet.talend.com" // this is needed for CDP build to resolve org.restlet.jee:org.restlet.ext.servlet:2.3.0
  val clouderaHadoopReleaseRepo = "Cloudera Hadoop Release" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  val clouderaReleaseLocalRepo  = "Cloudera Release Local" at "https://repository.cloudera.com/artifactory/libs-release-local/"
  val repo1Maven2               = "Repo1 Maven2" at "https://repo1.maven.org/maven2/"

  val typesafeReleaseRepo = "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/"
  val confluent           = "confluent" at "https://packages.confluent.io/maven/"

  /** custom resolvers for dependencies */
  val resolvers = Seq(
    mavenLocalRepo,
    restletMavenRepo,
    clouderaReleaseLocalRepo,
    clouderaHadoopReleaseRepo,
    repo1Maven2,
    confluent
  ) ++ sonatypeReleaseRepos ++ sonatypeSnapshotsRepos
}

class BasicSettings(
    resolver: Resolvers,
    jdkVersionValue: String,
    scalaVersionValue: ScalaVersion,
    overrideDep: Seq[ModuleID] = Seq.empty,
    exclustionRules: Seq[ExclusionRule] = Seq.empty
) extends Settings {

  /** settings related to project information */
  lazy val projectSettings = Seq(
    organization := "it.agilelab",
    organizationHomepage := Some(url("http://www.agilelab.it")),
    homepage := Some(url("https://www.agilelab.it/wasp-wide-analytics-streaming-platform/")),
    scmInfo := Some(ScmInfo(url("https://github.com/agile-lab-dev/wasp"), "https://github.com/agile-lab-dev/wasp.git")),
    developers := List(
      Developer("AgileLabDev", "AgileLabDev", "wasp@agilelab.it", url("https://github.com/agile-lab-dev/wasp"))
    ),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    dependencyOverrides ++= overrideDep
  )

  /** base build settings */
  lazy val buildSettings = Seq(
    resolvers ++= resolver.resolvers,
    exportJars := true,
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      s"-target:jvm-${jdkVersionValue}",
      "-feature",
      "-language:_",
      "-deprecation",
      "-unchecked",
      //"-Ylog-classpath",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Xfatal-warnings",
      "-Ywarn-inaccessible",
      "-Ywarn-unused-import",
      "-Ywarn-infer-any"
    ),
    javacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-source",
      jdkVersionValue,
      "-target",
      jdkVersionValue,
      "-Xlint:deprecation",
      "-Xlint:unchecked"
    ),
    doc / javacOptions --= Seq(
      "-target",
      jdkVersionValue,
      "-Xlint:deprecation",
      "-Xlint:unchecked"
    ),
    Compile / doc / scalacOptions --= Seq(
      "-Xfatal-warnings"
    ),
    libraryDependencies ++= {
      val silencerVersion = "1.4.3" // compatible with 2.11.12 and 2.12.10
      if (scalaVersionValue.isMajorMinor(2, 11) || (scalaVersionValue.isMajorMinor(2, 12) && scalaVersionValue.revision < 13)) {
        Seq(
          compilerPlugin("com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full),
          "com.github.ghik"        % "silencer-lib"             % silencerVersion % Provided cross CrossVersion.full
        )
      } else {
        Seq()
      }
    },
    scalaVersion := scalaVersionValue.raw,
    excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api"),
    excludeDependencies ++= this.exclustionRules


  )
  lazy val publishSettings = Seq(
    sonatypeBundleDirectory := (ThisBuild / baseDirectory).value / target.value.getName / "sonatype-staging" / (ThisBuild / version).value,
    sonatypeSessionName := s"[sbt-sonatype] ${name.value} ${scalaVersion.value} ${version.value}",
    publishTo := sonatypePublishToBundle.value,
    publishMavenStyle := true,
    credentials := Seq(sonatypeOssCredentials),
    pgpPassphrase := Option(System.getenv().get("PGP_PASSPHRASE")).map(_.toCharArray),
    Global / useGpg := false
  )

  /** settings for tests */
  lazy val testSettings = Seq(
    (Test / logBuffered) := true
  )

  /** common settings for all modules */
  lazy val commonSettings = projectSettings ++ buildSettings ++ publishSettings ++ testSettings

  /** sbt-buildinfo plugin configuration */
  lazy val sbtBuildInfoSettings: Seq[Def.Setting[_]] = Seq(
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      ("jdkVersion", jdkVersionValue),
      sbtVersion,
      gitCommitAction,
      gitWorkDirStatusAction,
      flavor
    ),
    buildInfoPackage := "it.agilelab.bigdata.wasp.core.build"
  )

  /** settings to disable parallel execution of tests, for Spark & co */
  lazy val disableParallelTests: Seq[Def.Setting[Boolean]] = Seq(
    (Test / parallelExecution) := false,
    (IntegrationTest / parallelExecution) := false
  )
  val sonatypeSnapshots = "SonatypeSnapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  val sonatypeStaging   = "SonatypeStaging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"
  val sonatypeOssCredentials = Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    System.getenv().get("SONATYPE_USER"),
    System.getenv().get("SONATYPE_PASSWORD")
  )

  /** sbt-buildinfo action to get current git commit */
  val gitCommitAction = BuildInfoKey.action[String]("gitCommitHash") {
    scala.sys.process.Process("git rev-parse HEAD").lineStream.head
  }

  /** sbt-buildinfo action to get git working directory status */
  val gitWorkDirStatusAction = BuildInfoKey.action[Boolean]("gitWorkDirStatus") {
    scala.sys.process.Process("git status --porcelain").lineStream.isEmpty
  }

  val flavor = BuildInfoKey.action[String]("flavor")(Flavor.currentFlavor().id)
}
