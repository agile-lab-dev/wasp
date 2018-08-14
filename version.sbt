import BranchingModelSupport._

val baseVersion = BaseVersion(2,16,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

