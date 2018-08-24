# Branching model

The branching model is as follows:

![branching-model](documentation/images/branching-model.png)

## Develop branch
This is the main development branch; it is always sync with the newest release.

When a build is performed on the develop branch, artifacts will be deployed with version `${BASE-VERSION}-SNAPSHOT`.

## Feature branches
These are the branches on which development of new features, and in general any work related to an issue, is performed. Feature branches have a 1-to-1 relationship with issues: one issue, one feature branch and vice versa.

The feature branches follow the naming scheme `feature/${issue-number}-{$description}`.

When a build is performed on a feature branch, artifacts will be deployed with version `${BASE-VERSION}-${issue-number}-SNAPSHOT`.

When a feature branch is ready it will be merged back into master

## Release branches
Release branches track releases.

When all the feature branches needed for a release have merged back to develop and the time comes for a new release, a new release branch branch is started from develop.

To backport a feature to a previous release the relevant commits are merged onto the release branch from the feature branch or cherrypicked form develop.

A release branch can be linked to sprints (linking the minor version to the sprint number) in order to guarantee a deliverable per week.

Release branches follow the naming scheme `release/v${major}.{minor}`.

When a build is performed on a release branch, artifacts will be deployed with version `${RELEASE-VERSION}-SNAPSHOT`.

## Hotfix branches
Hotfix branches are meant for quick, urgent fixes.

Hotfix branches start from release branches and merge back into them directly, without going through develop. If the fix is not already in develop, it is also merged into develop.

Hotfix branches follow the naming scheme `hotfix/${issue-number}-${description}`.

When a build is performed on a hotfix branch, artifacts will be deployed with version `${RELEASE-VERSION}-${issue-number}-hotfix-SNAPSHOT`.

## Tags
Tags help identify the precise commit which defines a version.
 
A tag follows the naming scheme `v${major}.${minor}.${patch}`.

When a build is performed on a tag, artifacts will be deployed with version
`v${major}.${minor}.${patch}`.

# Workflow
To start a new branch please open a new issue on GitLab, then after the issue is created please click on the `Create merge request` dropdown and change the proposed branch name
to one of the legal prefixes described in this document or you will not be able to push.

Do your work on the branch, then:
- rebase on the branch you started on
- merge your branch
- wait for the CI to deploy the artifact