import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,4)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

