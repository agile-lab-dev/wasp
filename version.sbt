import BranchingModelSupport._

val baseVersion = BaseVersion(2,16,1)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

