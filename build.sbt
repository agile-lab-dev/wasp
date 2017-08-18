/*
 * Main build definition.
 *
 * See project/Settings.scala for the settings definitions.
 * See project/Dependencies.scala for the dependencies definitions.
 * See project/Versions.scala for the versions definitions.
 */
lazy val core = Project("wasp-core", file("core"))
	.settings(Settings.commonSettings:_*)
	.settings(Settings.sbtBuildInfoSettings:_*)
	.settings(libraryDependencies ++= Dependencies.core)
	.enablePlugins(BuildInfoPlugin)

lazy val master = Project("wasp-master", file("master"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.master)
	.enablePlugins(JavaAppPackaging)

lazy val producers = Project("wasp-producers", file("producers"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.producers)
	.enablePlugins(JavaAppPackaging)

lazy val consumers_spark = Project("wasp-consumers-spark", file("consumers-spark"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.consumers_spark)
	.enablePlugins(JavaAppPackaging)

lazy val consumers_rt = Project("wasp-consumers-rt", file("consumers-rt"))
	.settings(Settings.commonSettings:_*)
	.dependsOn(core)
	.settings(libraryDependencies ++= Dependencies.consumers_rt)
	.enablePlugins(JavaAppPackaging)

lazy val wasp = Project("wasp", file("."))
	.settings(Settings.commonSettings:_*)
	.aggregate(core, master, producers, consumers_spark, consumers_rt)
