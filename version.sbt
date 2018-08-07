import BranchingModelSupport._

val baseVersion = BaseVersion(2,15,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

