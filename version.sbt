import BranchingModelSupport._

val baseVersion = BaseVersion(2,24,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

