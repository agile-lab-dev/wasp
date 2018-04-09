import BranchingModelSupport._

val baseVersion = BaseVersion(2,10,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

