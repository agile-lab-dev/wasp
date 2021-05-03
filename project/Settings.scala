import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys._
import sbtbuildinfo.BuildInfoKey
import com.typesafe.sbt.pgp.PgpSettings.pgpPassphrase


/*
 * Common settings for all the modules.
 *
 * See project/Versions.scala for the versions definitions.
 */
object Resolvers {
  val mavenLocalRepo            = Resolver.mavenLocal
  val sonatypeReleaseRepo       = Resolver.sonatypeRepo("releases")
  val sonatypeSnapshotsRepo       = Resolver.sonatypeRepo("snapshots")
  val bintraySparkSolrRepo      = Resolver.bintrayRepo("agile-lab-dev", "Spark-Solr")
  val restletMavenRepo          = "Restlet Maven repository" at "https://maven.restlet.com/"
  val clouderaHadoopReleaseRepo = "Cloudera Hadoop Release" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  val clouderaReleaseLocalRepo  = "Cloudera Release Local" at "https://repository.cloudera.com/artifactory/libs-release-local/"
  val repo1Maven2               = "Repo1 Maven2" at "https://repo1.maven.org/maven2/"

  val typesafeReleaseRepo       = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
  val confluent                 = "confluent" at "https://packages.confluent.io/maven/"

  /** custom resolvers for dependencies */
  val resolvers = Seq(
    mavenLocalRepo,
    sonatypeReleaseRepo,
    sonatypeSnapshotsRepo,
    bintraySparkSolrRepo,
    //typesafeReleaseRepo,
    restletMavenRepo,
    clouderaReleaseLocalRepo,
    clouderaHadoopReleaseRepo,
    repo1Maven2,
    confluent
  )
}

object Settings {

  /** settings related to project information */
  lazy val projectSettings = Seq(
    organization := "it.agilelab",
    organizationHomepage := Some(url("http://www.agilelab.it")),
    homepage := Some(url("https://www.agilelab.it/wasp-wide-analytics-streaming-platform/")),
    scmInfo := Some(ScmInfo(url("https://github.com/agile-lab-dev/wasp"), "https://github.com/agile-lab-dev/wasp.git")),
    developers := List(Developer("AgileLabDev", "AgileLabDev", "wasp@agilelab.it", url("https://github.com/agile-lab-dev/wasp"))),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
  )

  /** base build settings */
  lazy val buildSettings = Seq(
    resolvers ++= Resolvers.resolvers,
    exportJars := true,
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      s"-target:jvm-${Versions.jdk}",
      "-feature",
      "-language:_",
      "-deprecation",
      "-unchecked",
      //"-Ylog-classpath",
      "-Xlint"
    ),
    javacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-source",
      Versions.jdk,
      "-target",
      Versions.jdk,
      "-Xlint:deprecation",
      "-Xlint:unchecked"

    ),
    doc / javacOptions --= Seq(
      "-target",
      Versions.jdk,
      "-Xlint:deprecation",
      "-Xlint:unchecked"
    ),
    scalaVersion := Versions.scala,
    excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
  )

  val sonatypeSnapshots = "SonatypeSnapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  val sonatypeStaging = "SonatypeStaging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"

  val sonatypeOssCredentials = Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    System.getenv().get("SONATYPE_USER"),
    System.getenv().get("SONATYPE_PASSWORD")
  )


  lazy val publishSettings = Seq(
    publishMavenStyle := true,
    updateOptions := updateOptions.value
      .withGigahorse(false), // workaround for publish fails https://github.com/sbt/sbt/issues/3570
    publishTo := {
      if (isSnapshot.value)
        Some(sonatypeSnapshots)
      else
        Some(sonatypeStaging)
    },
    credentials := Seq(sonatypeOssCredentials),
    pgpPassphrase := Option(System.getenv().get("GPG_PASSPHRASE")).map(_.toCharArray)
  )

  /** settings for tests */
  lazy val testSettings = Seq(
    logBuffered in Test := true
  )

  /** common settings for all modules */
  lazy val commonSettings = projectSettings ++ buildSettings ++ publishSettings ++ testSettings

  /** sbt-buildinfo action to get current git commit */
  val gitCommitAction = BuildInfoKey.action[String]("gitCommitHash") {
    scala.sys.process.Process("git rev-parse HEAD").lineStream.head
  }

  /** sbt-buildinfo action to get git working directory status */
  val gitWorkDirStatusAction = BuildInfoKey.action[Boolean]("gitWorkDirStatus") {
    scala.sys.process.Process("git status --porcelain").lineStream.isEmpty
  }

  /** sbt-buildinfo plugin configuration */
  lazy val sbtBuildInfoSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      ("jdkVersion", Versions.jdk),
      sbtVersion,
      gitCommitAction,
      gitWorkDirStatusAction
    ),
    buildInfoPackage := "it.agilelab.bigdata.wasp.core.build"
  )

  /** settings to disable parallel execution of tests, for Spark & co */
  lazy val disableParallelTests = Seq(
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false
  )
}
