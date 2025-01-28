// sbt-buildinfo, for accessing build information in the code - https://github.com/sbt/sbt-buildinfo/
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.1")
// sbt-native-packager, used for assembly jars for the start-wasp script
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.0")
// test coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.3")
// perform release on sonatype
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.21")
// sign artifacts
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
addDependencyTreePlugin

// in this way it does not complain if we have two different versions of scala-xml (sbt-scoverage here is the issue)
// but we can't upgrade it to 2.x because it drops scala 2.11 support
libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
