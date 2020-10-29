import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,1)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

