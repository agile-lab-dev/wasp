import BranchingModelSupport._

val baseVersion = BaseVersion(2,20,0)

version in ThisBuild := versionForContainingRepositoryOrGitlabCi(baseVersion)

