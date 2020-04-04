import BranchingModelSupport._

val baseVersion = BaseVersion(2,17,5)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

