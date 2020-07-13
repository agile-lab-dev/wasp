#!/usr/bin/env bash
chmod 777 -R .
useradd -m ${UNPRIVILEGED_USER}
# shellcheck disable=SC2068
COMMAND="sbt $@"
su $UNPRIVILEGED_USER -c "${COMMAND}"