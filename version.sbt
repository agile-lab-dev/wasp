import BranchingModelSupport._

val baseVersion = BaseVersion(2,12,4)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

