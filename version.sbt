import BranchingModelSupport._

val baseVersion = BaseVersion(2,13,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

