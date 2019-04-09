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

	/** settings related to project information */
	lazy val projectSettings = Seq(
		organization := "it.agilelab",
		organizationHomepage := Some(url("http://www.agilelab.it")),
		homepage := Some(url("http://www.agilelab.it")),
		licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
	)

	/** Return the local Maven resolver
		*
		* To use in order to resolve dependecies in Maven local repository (e.g. during the test of new versions of Spark-Solr lib)
		*
		* */
	val mavenLocalRepo = Resolver.mavenLocal
	val sonatypeReleaseRepo = Resolver.sonatypeRepo("releases")
	val bintraySparkSolrRepo = Resolver.bintrayRepo("agile-lab-dev", "Spark-Solr")
	val typesafeReleaseRepo = "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
	val restletMavenRepo = "Restlet Maven repository" at "https://maven.restlet.com/"
	val clouderaHadoopReleaseRepo = "Cloudera Hadoop Release" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
	
	/** custom resolvers for dependencies */
	lazy val customResolvers = Seq(
		//mavenLocalRepo,
		sonatypeReleaseRepo,
		bintraySparkSolrRepo,
		typesafeReleaseRepo,
		restletMavenRepo,
		clouderaHadoopReleaseRepo
	)

	/** base build settings */
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
			"-Ylog-classpath",
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
	
	val jfrogOssSnapshotRepo = "Agile Lab JFrog Snapshot OSS" at "https://oss.jfrog.org/artifactory/oss-snapshot-local"
	val jfrogOssReleaseRepo = "Agile Lab JFrog Release OSS" at "https://oss.jfrog.org/artifactory/oss-release-local"

	val jfrogOssCredentials = Credentials(
		"Artifactory Realm",
		"oss.jfrog.org",
		System.getenv().get("BINTRAY_USERNAME"),
		System.getenv().get("BINTRAY_API_KEY")
	)

	lazy val publishSettings = Seq(
		publishMavenStyle := true,
		updateOptions := updateOptions.value.withGigahorse(false), // workaround for publish fails https://github.com/sbt/sbt/issues/3570
		publishTo := {
			if (isSnapshot.value)
				Some(jfrogOssSnapshotRepo)
			else
				Some(jfrogOssReleaseRepo)
		},
		credentials := Seq(jfrogOssCredentials)
	)
	
	/** settings for tests */
	lazy val testSettings = Seq(
		logBuffered in Test := false // disable buffering to make scalatest output immediate
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
		buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, ("jdkVersion", Versions.jdk), sbtVersion, gitCommitAction, gitWorkDirStatusAction),
		buildInfoOptions += BuildInfoOption.BuildTime,
		buildInfoPackage := "it.agilelab.bigdata.wasp.core.build"
	)
	
	/** settings to disable parallel execution of tests, for Spark & co */
	lazy val disableParallelTests = Seq(
		parallelExecution in Test := false,
		parallelExecution in IntegrationTest := false
	)
}
