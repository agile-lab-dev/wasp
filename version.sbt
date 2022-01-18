import BranchingModelSupport._

val baseVersion = BaseVersion(2,27,3)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

