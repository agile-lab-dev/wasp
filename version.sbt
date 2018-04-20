import BranchingModelSupport._

val baseVersion = BaseVersion(2,12,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

