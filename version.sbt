import BranchingModelSupport._

val baseVersion = BaseVersion(2,29,0)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

