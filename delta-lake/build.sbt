import sbt.Keys.scalacOptions

scalacOptions --= Seq(
  "-Ywarn-unused:imports",
  "-Xfatal-warnings",
  "-Xcheckinit",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused",
  "-deprecation",
  "-Ywarn-value-discard",
  "-Ywarn-numeric-widen",
  "-Xlint:deprecation"
)
