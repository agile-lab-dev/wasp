import BranchingModelSupport._

val baseVersion = BaseVersion(2,27,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

