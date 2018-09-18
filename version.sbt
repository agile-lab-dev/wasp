import BranchingModelSupport._

val baseVersion = BaseVersion(2,17,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

