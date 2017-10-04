// sbt-buildinfo, for accessing build information in the code - https://github.com/sbt/sbt-buildinfo/
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
// sbt-native, used for assembly jars for the start-wasp script
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.2")
// sbt-release, used for create the project release
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.6")
// sbt-bintray, used for publish the artifacts to bintray
//addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.1")
