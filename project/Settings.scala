import sbt._
import sbt.Keys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}
import sbtbuildinfo.BuildInfoKeys._

/*
 * Common settings for all the modules.
 *
 * See project/Versions.scala for the versions definitions.
 */
object Settings {
	// settings related to project information
	lazy val projectSettings = Seq(
		name := "WASP",
		normalizedName := "wasp",
		organization := "it.agilelab.bigdata.wasp",
		organizationHomepage := Some(url("http://www.agilelab.it")),
		homepage := Some(url("http://www.agilelab.it")),
		licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
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
		scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.jdk}", "-feature", "-language:_", "-deprecation", "-unchecked", "-Xlint"),
		javacOptions ++= Seq("-encoding", "UTF-8", "-source", Versions.jdk, "-target", Versions.jdk, "-Xlint:deprecation", "-Xlint:unchecked"),
		scalaVersion := Versions.scala
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