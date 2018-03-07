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
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
	)
	
	// custom resolvers for dependencies
	lazy val customResolvers = Seq(
		Resolver.mavenLocal,
		"gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Agile Lab Spark-Solr Bintray" at "https://dl.bintray.com/agile-lab-dev/Spark-Solr/",
    "Restlet Maven repository" at "https://maven.restlet.com/",
    "Cloudera Hadoop Releases" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
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
	
  // target repo & credentials for publishing to bintray
  val bintrayRepoHostname = "api.bintray.com"
	val bintrayRepo = "Agile Lab WASP Bintray" at s"https://$bintrayRepoHostname/maven/agile-lab-dev/WASP/wasp/;publish=1"
	val bintrayCredentials = Credentials("Bintray API Realm", bintrayRepoHostname, System.getenv().get("BINTRAY_USERNAME"), System.getenv().get("BINTRAY_API_KEY"))
	// publishing settings
	lazy val publishingSettings = Seq(
		publishMavenStyle := true,
		updateOptions := updateOptions.value.withGigahorse(false), // workaround for publish fails https://github.com/sbt/sbt/issues/3570
		publishTo := Some(bintrayRepo),
		credentials := Seq(bintrayCredentials)
	)
	
	// common settings for all modules
	lazy val commonSettings: Seq[Def.SettingsDefinition] = projectSettings ++ buildSettings ++ publishingSettings
	
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