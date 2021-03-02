import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,7)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

