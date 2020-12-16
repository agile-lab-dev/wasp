import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,3)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

