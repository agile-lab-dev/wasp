import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,2)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

