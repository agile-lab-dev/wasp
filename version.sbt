import BranchingModelSupport._

val baseVersion = BaseVersion(2,8,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

