import BranchingModelSupport._

val baseVersion = BaseVersion(2,20,1)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

