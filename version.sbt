import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,6)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

