lazy val commonSettings = Seq(
	name := "WASP",
	normalizedName := "wasp",
	organization := "it.agilelab.bigdata.wasp",
	organizationHomepage := Some(url("http://www.agilelab.it")),
	homepage := Some(url("http://www.agilelab.it")),
	licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
	version := "2.0.0-SNAPSHOT",
	scalaVersion := "2.11.11"
)

lazy val core = Project("wasp-core", file("core"))
	.settings(commonSettings)

lazy val master = Project("wasp-master", file("master"))
	.settings(commonSettings)

lazy val producers = Project("wasp-producers", file("producers"))
	.settings(commonSettings)

lazy val consumers_spark = Project("wasp-consumers-spark", file("consumers-spark"))
	.settings(commonSettings)

lazy val consumers_rt = Project("wasp-consumers-rt", file("consumers-rt"))
	.settings(commonSettings)

lazy val wasp = Project("wasp", file("."))
	.settings(commonSettings)
	.aggregate(core, master, producers, consumers_spark, consumers_rt)
