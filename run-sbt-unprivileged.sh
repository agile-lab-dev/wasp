#!/usr/bin/env bash
# this script is needed because otherwise postgresql embedded does not work
chmod 777 -R .
useradd -m ${UNPRIVILEGED_USER}
# the next two lines are needed because newer git versions will complain
# if the user that invokes git command does not own the .git folder and its
# parent
chown ${UNPRIVILEGED_USER} .
chown ${UNPRIVILEGED_USER} -R .git
# shellcheck disable=SC2068
COMMAND="sbt $@"
su $UNPRIVILEGED_USER -c "${COMMAND}"