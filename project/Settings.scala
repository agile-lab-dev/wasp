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
	lazy val customResolvers: Seq[MavenRepository] = Seq(
		mavenLocal,
		Resolver.sonatypeRepo("releases"),
		Resolver.bintrayRepo("agile-lab-dev", "Spark-Solr"),
		"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
		"Restlet Maven repository" at "https://maven.restlet.com/",
		"Cloudera Hadoop Releases" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
	)

	/** Return the local Maven resolver
		*
		* To use in order to resolve dependecies in Maven local repository (e.g. during the test of new versions of Spark-Solr lib)
		*
		* */
	def mavenLocal: MavenRepository = Resolver.mavenLocal


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


	val jfrogOssReleaseRepo = "Agile Lab JFrog Releases OSS" at "https://oss.jfrog.org/artifactory/oss-release-local"
	val jfrogOssSnapshotRepo = "Agile Lab JFrog Snapshots OSS" at "https://oss.jfrog.org/artifactory/oss-snapshot-local"

	val jfrogOssCredentials = Credentials("Artifactory Realm", "oss.jfrog.org", System.getenv().get
	("BINTRAY_USERNAME"), System.getenv().get("BINTRAY_API_KEY"))

	lazy val publishingSettings = Seq(
		publishMavenStyle := true,
		updateOptions := updateOptions.value.withGigahorse(false), // workaround for publish fails https://github.com/sbt/sbt/issues/3570
		publishTo := {
			if (isSnapshot.value){
				Some(jfrogOssSnapshotRepo)
			}else{
				Some(jfrogOssReleaseRepo)

			}
		},
		credentials := Seq(jfrogOssCredentials)
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