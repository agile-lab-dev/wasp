import BranchingModelSupport._

val baseVersion = BaseVersion(2,23,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

