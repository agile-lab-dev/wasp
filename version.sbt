import BranchingModelSupport._

val baseVersion = BaseVersion(2,21,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

