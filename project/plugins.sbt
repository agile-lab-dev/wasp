// sbt-buildinfo, for accessing build information in the code - https://github.com/sbt/sbt-buildinfo/
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.10.0")
// sbt-native-packager, used for assembly jars for the start-wasp script
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.1")
// test coverage
addSbtPlugin("org.scoverage"  % "sbt-scoverage" % "1.9.0")
// perform release on sonatype
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.10")
// sign artifacts
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addDependencyTreePlugin
