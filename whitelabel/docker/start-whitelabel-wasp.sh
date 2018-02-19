#!/usr/bin/env bash

SBT_STAGE_COMMAND_PROJECTID="wasp-whitelabel/" \
MASTER_PROJECT_DIRECTORY="/../master/" \
PRODUCERS_PROJECT_DIRECTORY="/../producers/" \
CONSUMERS_RT_PROJECT_DIRECTORY="/../consumers-rt/" \
CONSUMERS_SPARK_PROJECT_DIRECTORY="/../consumers-spark/" \
MASTER_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-master" \
PRODUCERS_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-producers" \
CONSUMERS_RT_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-consumers-rt" \
CONSUMERS_SPARK_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-consumers-spark" \
./start-wasp.sh $@