# Release

Releasing a new version of Witboost Data Stream is mainly handled remotely by our beloved CI/CD pipeline on Gitlab.

Anyway there are still some semi-automatic tasks, like bumping the version, tagging the commits, creating branches and pushing to the remote repository.

Base version is kept inside `baseVersion.version` in the format `${major}.${minor}.${patch}`.

## Release minor versions

As highlighted in the [contributing page](CONTRIBUTING.md) we keep long-running release branches for minor releases in order to perform hotfixes.
In order to do so, a `release.sh` script under `sbin` directory is implemented to get the branch/tag creation and version bump right every time!

The scripts needs `sbt`, `git`, `head` and `tail` installed locally and also push grants on MAIN BRANCH for the current user.

The process will do the following (and stop when something goes bad) so you can manually recover.

1. fetch the remote repository
2. check pre-conditions (you are on the main branch, it is up to date and your working directory is clean (except for untracked files))
3. get the current version and wait for user confirmation (using sbt)
4. tag the current ref with the `v${version}` convention
5. bump the minor version (on main branch)
6. commit and push the minor version change and the version tag
7. create the release branch with the `release/v{major.minor}` convention off the just tagged version
8. bump the patch version (on release branch)
9. commit and push the patch version change and the version tag
