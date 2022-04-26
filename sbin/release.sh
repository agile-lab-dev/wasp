#!/bin/bash

set -ex
export SBT_NATIVE_CLIENT=false
SBT_ARGS='--no-colors --error'
MAIN_BRANCH='develop'
NOT_ON_MAIN_BRANCH_EXIT_CODE=1
UNTRACKED_FILES_EXIT_CODE=2
NOT_UP_TO_DATE_BRANCH_EXIT_CODE=3

echo "Fetch remote git repository..."
git fetch

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_REMOTE_BRANCH=$(git rev-parse --abbrev-ref --symbolic-full-name @{u} | head -n 1)
CURRENT_SHA=$(git rev-parse HEAD)
REMOTE_SHA=$(git rev-parse $CURRENT_REMOTE_BRANCH)

if [ $CURRENT_BRANCH == $MAIN_BRANCH ]; then
  echo "You are on main branch: $CURRENT_BRANCH"
else
  echo "You are not on $MAIN_BRANCH aborting"
  exit $NOT_ON_MAIN_BRANCH_EXIT_CODE
fi


if [ $CURRENT_SHA == $REMOTE_SHA ]; then
  echo "Current branch ($CURRENT_BRANCH) is up to date with remote branch ($CURRENT_REMOTE_BRANCH)"
else
  echo "Current branch ($CURRENT_BRANCH) is up NOT to date with remote branch ($CURRENT_REMOTE_BRANCH)"
  echo "Please run 'git pull'"
  exit $NOT_UP_TO_DATE_BRANCH_EXIT_CODE
fi

UNTRACKED_FILES="$(git status --untracked-files=no --porcelain)"
if [ -z "$UNTRACKED_FILES" ]; then 
  echo "Your working directory is clean"
else 
  echo "Your working directory is dirty:"
  echo "$UNTRACKED_FILES"
  echo "Aborting.."
  exit $UNTRACKED_FILES_EXIT_CODE
fi

ALL_OUTPUT=$(sbt $SBT_ARGS "print majorMinor;print majorMinorPatch")
majorMinor=$(echo "$ALL_OUTPUT" | head -n 1)
version=$(echo "$ALL_OUTPUT" | head -n 2 | tail -n 1)

echo "Current version is $version"
echo "Current major minor version is $majorMinor"
echo "Starting the release process"
echo "Press enter to continue"
read

#### CREATE TAG
TAG_NAME="v$version"
git tag $TAG_NAME $CURRENT_SHA

#### BUMP MINOR ON MAIN BRANCH ####
sbt "bumpMinor"
bumpedVersion=$(sbt $SBT_ARGS "print majorMinorPatch")
git add baseVersion.version
git commit -m "Bump to version $bumpedVersion"
git push && git push --tags

#### CREATE RELEASE
releaseBranch="release/v$majorMinor"
git checkout -b $releaseBranch $TAG_NAME

#### BUMP PATCH ON RELEASE BRANCH
sbt "bumpPatch"
bumpedVersion=$(sbt $SBT_ARGS "print majorMinorPatch")
git add baseVersion.version
git commit -m "Bump to version $bumpedVersion"
git push --set-upstream origin $releaseBranch

echo "done!"
echo "Have fun!"