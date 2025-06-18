// sbt-buildinfo, for accessing build information in the code - https://github.com/sbt/sbt-buildinfo/
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.1")
// sbt-native-packager, used for assembly jars for the start-wasp script
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.0")
// test coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.3.0")
// sign artifacts
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
// scalafmt
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
addDependencyTreePlugin
