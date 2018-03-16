import BranchingModelSupport._

val baseVersion = BaseVersion(2,7,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

