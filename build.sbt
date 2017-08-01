import Settings.commonSettings

/*
 * Main build definition.
 *
 * See project/Settings.scala for the settings definitions.
 * See project/Dependencies.scala for the dependencies definitions.
 * See project/Versions.scala for the versions definitions.
 */
lazy val core = Project("wasp-core", file("core"))
	.settings(commonSettings:_*)

lazy val master = Project("wasp-master", file("master"))
	.settings(commonSettings:_*)

lazy val producers = Project("wasp-producers", file("producers"))
	.settings(commonSettings:_*)

lazy val consumers_spark = Project("wasp-consumers-spark", file("consumers-spark"))
	.settings(commonSettings:_*)

lazy val consumers_rt = Project("wasp-consumers-rt", file("consumers-rt"))
	.settings(commonSettings:_*)

lazy val wasp = Project("wasp", file("."))
	.settings(commonSettings:_*)
	.aggregate(core, master, producers, consumers_spark, consumers_rt)
