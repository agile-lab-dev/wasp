import sbt._
import sbt.Keys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}
import sbtbuildinfo.BuildInfoKeys._
import bintray.BintrayKeys._


/*
 * Common settings for all the modules.
 *
 * See project/Versions.scala for the versions definitions.
 */
object Settings {
	// settings related to project information
	lazy val projectSettings = Seq(
		organization := "it.agilelab.bigdata.wasp",
		organizationHomepage := Some(url("http://www.agilelab.it")),
		homepage := Some(url("http://www.agilelab.it")),
		licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
		bintrayOrganization := Some("agile-lab-dev"),
		bintrayRepository := "WASP", // target repo
		bintrayPackage := "wasp", // target package
		bintrayReleaseOnPublish := false // do not automatically release, instead do sbt publish, then sbt bintrayRelease
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
	
	// global exclusions for slf4j implementations and the like
	lazy val globalExclusions = Seq(
		SbtExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
	)
	
	// base build settings
	lazy val buildSettings = Seq(
		resolvers ++= customResolvers,
		exportJars := true,
		scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.jdk}", "-feature", "-language:_", "-deprecation", "-unchecked", "-Xlint"),
		javacOptions ++= Seq("-encoding", "UTF-8", "-source", Versions.jdk, "-target", Versions.jdk, "-Xlint:deprecation", "-Xlint:unchecked"),
		scalaVersion := Versions.scala,
		excludeDependencies ++= globalExclusions
	)
	
	// common settings for all modules
	lazy val commonSettings: Seq[Def.SettingsDefinition] = projectSettings ++ buildSettings
	
	// sbt-buildinfo action to get current git commit
	val gitCommitAction = BuildInfoKey.action[String]("gitCommitHash") { Process("git rev-parse HEAD").lines.head }
	
	// sbt-buildinfo action to get git working directory status
	val gitWorkDirStatusAction = BuildInfoKey.action[Boolean]("gitWorkDirStatus") { Process("git status --porcelain").lines.isEmpty }
	
	// sbt-buildinfo plugin configuration
	lazy val sbtBuildInfoSettings = Seq(
		buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, ("jdkVersion", Versions.jdk), sbtVersion, gitCommitAction, gitWorkDirStatusAction),
		buildInfoOptions += BuildInfoOption.BuildTime,
		buildInfoPackage := "it.agilelab.bigdata.wasp.core.build"
	)
}