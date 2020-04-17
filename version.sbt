import BranchingModelSupport._

val baseVersion = BaseVersion(2,26,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

