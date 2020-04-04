import BranchingModelSupport._

val baseVersion = BaseVersion(2,19,10)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

