import BranchingModelSupport._

val baseVersion = BaseVersion(2,18,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

