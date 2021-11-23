import BranchingModelSupport._

val baseVersion = BaseVersion(2,27,1)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

