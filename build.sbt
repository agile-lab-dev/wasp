/*
 * Main build definition.
 *
 * See project/Settings.scala for the settings definitions.
 * See project/Dependencies.scala for the dependencies definitions.
 * See project/Versions.scala for the versions definitions.
 */
lazy val core = Project("wasp-core", file("core"))
	.settings(Settings.commonSettings:_*)
	.settings(libraryDependencies ++= Dependencies.core)
	.enablePlugins(BuildInfoPlugin)
	.settings(Settings.sbtBuildInfoSettings:_*)

lazy val master = Project("wasp-master", file("master"))
	.settings(Settings.commonSettings:_*)

lazy val producers = Project("wasp-producers", file("producers"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.producers)

lazy val consumers_spark = Project("wasp-consumers-spark", file("consumers-spark"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.consumers_spark)

lazy val consumers_rt = Project("wasp-consumers-rt", file("consumers-rt"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.consumers_rt)

lazy val wasp = Project("wasp", file("."))
	.settings(Settings.commonSettings:_*)
	.aggregate(core, master, producers, consumers_spark, consumers_rt)
