#!/usr/bin/env bash
# this script is needed because otherwise postgresql embedded does not work
chmod 777 -R .
useradd -m ${UNPRIVILEGED_USER}
# shellcheck disable=SC2068
COMMAND="sbt $@"
su $UNPRIVILEGED_USER -c "${COMMAND}"