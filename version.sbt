import BranchingModelSupport._

val baseVersion = BaseVersion(2,14,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

