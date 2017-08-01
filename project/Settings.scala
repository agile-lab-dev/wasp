import sbt._
import sbt.Keys._
import sbtbuildinfo.BuildInfoKey
import sbtbuildinfo.BuildInfoKeys._

/*
 * Common settings for all the modules.
 *
 * See project/Versions.scala for the versions definitions.
 */
object Settings {
	lazy val projectSettings = Seq(
		name := "WASP",
		normalizedName := "wasp",
		organization := "it.agilelab.bigdata.wasp",
		organizationHomepage := Some(url("http://www.agilelab.it")),
		homepage := Some(url("http://www.agilelab.it")),
		licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
	)
	
	lazy val customResolvers = Seq(
		Resolver.mavenLocal,
		"gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Bintray" at "https://dl.bintray.com/agile-lab-dev/Spark-Solr/",
    "Restlet Maven repository" at "https://maven.restlet.com/",
    "Hadoop Releases" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    Resolver.sonatypeRepo("releases")
	)
	
	lazy val buildSettings = Seq(
		resolvers ++= customResolvers,
		exportJars := true,
		scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.jdk}", "-feature", "-language:_", "-deprecation", "-unchecked", "-Xlint"),
		javacOptions ++= Seq("-encoding", "UTF-8", "-source", Versions.jdk, "-target", Versions.jdk, "-Xlint:deprecation", "-Xlint:unchecked"),
		scalaVersion := Versions.scala
	)
	
	lazy val commonSettings: Seq[Def.SettingsDefinition] = projectSettings ++ buildSettings
	
	// sbt-buildinfo plugin configuration
	lazy val sbtBuildInfoSettings = Seq(
		buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
		//buildInfoOptions += BuildInfoOption.BuildTime,
		buildInfoPackage := "it.agilelab.bigdata.wasp.core.build",
		sourceDirectories in Compile += target.value / "/src_managed/main/sbt-buildinfo"
	)
}