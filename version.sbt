import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,5)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

