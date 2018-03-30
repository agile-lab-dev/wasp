import BranchingModelSupport._

val baseVersion = BaseVersion(2,9,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

