import BranchingModelSupport._

val baseVersion = BaseVersion(2,5,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

