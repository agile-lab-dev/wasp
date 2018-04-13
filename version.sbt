import BranchingModelSupport._

val baseVersion = BaseVersion(2,11,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

