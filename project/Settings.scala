//import bintray.BintrayKeys._
import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}


/*
 * Common settings for all the modules.
 *
 * See project/Versions.scala for the versions definitions.
 */

object Settings {
	// settings related to project information
	lazy val projectSettings = Seq(
		organization := "it.agilelab",
		organizationHomepage := Some(url("http://www.agilelab.it")),
		homepage := Some(url("http://www.agilelab.it")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
		publishMavenStyle := true,
    publishArtifact in(Compile, packageDoc) := false,
		publishTo := {
			val nexus = System.getenv().get("NEXUS_URL") + "/"
			if (isSnapshot.value)
				Some("snapshots" at nexus + "repository/maven-snapshots")
			else
				Some("releases"  at nexus + "repository/maven-releases")
    },
    // Workaround for publish fails https://github.com/sbt/sbt/issues/3570
    updateOptions := updateOptions.value.withGigahorse(false),
    credentials ++= (for {
      user <- Option(System.getenv().get("NEUXS_USERNAME"))
      pw <- Option(System.getenv().get("NEUXS_PASSWORD"))
    } yield Credentials("Sonatype Nexus Repository Manager", System.getenv().get("NEXUS_DOMAIN"), user, pw)).toSeq
	)
	
	// custom resolvers for dependencies
	lazy val customResolvers = Seq(
		Resolver.mavenLocal,
		"gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Bintray" at "https://dl.bintray.com/agile-lab-dev/Spark-Solr/",
    "Restlet Maven repository" at "https://maven.restlet.com/",
    "Hadoop Releases" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    Resolver.sonatypeRepo("releases")
	)
	
	// base build settings
	lazy val buildSettings = Seq(
		resolvers ++= customResolvers,
		exportJars := true,
		scalacOptions ++= Seq(
			"-encoding", "UTF-8",
			s"-target:jvm-${Versions.jdk}",
			"-feature",
			"-language:_",
			"-deprecation",
			"-unchecked",
			"-Xlint",
			"-Xmax-classfile-name", "72"),
		javacOptions ++= Seq(
			"-encoding", "UTF-8",
			"-source", Versions.jdk,
			"-target", Versions.jdk,
			"-Xlint:deprecation",
			"-Xlint:unchecked"),
		scalaVersion := Versions.scala
	)

  // settings for the bintray-sbt plugin used to publish to Bintray
  // set here because "in ThisBuild" scoping doesn't work!
/*  lazy val bintraySettings = Seq(
    bintrayOrganization := Some("agile-lab-dev"),
    bintrayRepository := "WASP", // target repo
    bintrayPackage := "wasp", // target package
    bintrayReleaseOnPublish := false // do not automatically release, instead do sbt publish, then sbt bintrayRelease
  )
*/
	// common settings for all modules
	lazy val commonSettings: Seq[Def.SettingsDefinition] = projectSettings ++ buildSettings //++ bintraySettings
	
	// sbt-buildinfo action to get current git commit
	val gitCommitAction = BuildInfoKey.action[String]("gitCommitHash") {
		scala.sys.process.Process("git rev-parse HEAD").lineStream.head
	}
	
	// sbt-buildinfo action to get git working directory status
	val gitWorkDirStatusAction = BuildInfoKey.action[Boolean]("gitWorkDirStatus") {
		scala.sys.process.Process("git status --porcelain").lineStream.isEmpty
	}
	
	// sbt-buildinfo plugin configuration
	lazy val sbtBuildInfoSettings = Seq(
		buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, ("jdkVersion", Versions.jdk), sbtVersion, gitCommitAction, gitWorkDirStatusAction),
		buildInfoOptions += BuildInfoOption.BuildTime,
		buildInfoPackage := "it.agilelab.bigdata.wasp.core.build"
	)
}