import BranchingModelSupport._

val baseVersion = BaseVersion(2,19,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

