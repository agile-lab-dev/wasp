import BranchingModelSupport._

val baseVersion =
  BaseVersion
    .parse(IO.read(file("baseVersion.version")))
    .fold(t => throw new RuntimeException(t), identity)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

val bumpMajor       = taskKey[String]("Bump major version of the project")
val bumpMinor       = taskKey[String]("Bump minor version of the project")
val bumpPatch       = taskKey[String]("Bump patch version of the project")
val majorMinor      = settingKey[String]("Print {major}.{minor} version").withRank(KeyRanks.Invisible)
val majorMinorPatch = settingKey[String]("Print {major}.{minor}.{patch} version").withRank(KeyRanks.Invisible)

bumpMajor := VersionBumper.bumpMajor(version.value, sLog.value).toString()
bumpMinor := VersionBumper.bumpMinor(version.value, sLog.value).toString()
bumpPatch := VersionBumper.bumpPatch(version.value, sLog.value).toString()
majorMinor := BranchingModelSupport.BaseVersion.parse(version.value).map(_.majorMinor).right.get
majorMinorPatch := BranchingModelSupport.BaseVersion.parse(version.value).map(_.majorMinorPatch).right.get
