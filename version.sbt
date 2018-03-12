import BranchingModelSupport._

val baseVersion = BaseVersion(2,6,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

