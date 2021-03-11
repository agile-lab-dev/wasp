import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,8)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

