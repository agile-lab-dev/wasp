import BranchingModelSupport._

val baseVersion = BaseVersion(2,15,2)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

