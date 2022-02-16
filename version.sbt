import BranchingModelSupport._

val baseVersion = BaseVersion(2,28,1)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

