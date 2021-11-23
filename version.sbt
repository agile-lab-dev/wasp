import BranchingModelSupport._

val baseVersion = BaseVersion(2,28,0)

(ThisBuild / version) := versionForContainingRepositoryOrGitlabCi(baseVersion, Flavor.currentFlavor())

