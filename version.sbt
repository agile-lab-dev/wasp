import BranchingModelSupport._

val baseVersion = BaseVersion(2,27,0)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

