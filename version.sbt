import BranchingModelSupport._

val baseVersion = BaseVersion(2,27,2)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

