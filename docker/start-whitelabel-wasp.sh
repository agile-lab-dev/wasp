#!/usr/bin/env bash

MASTER_PROJECT_DIRECTORY="/../whitelabel/master/" \
CONSUMERS_SPARK_PROJECT_DIRECTORY="/../whitelabel/consumers-spark/" \
CONSUMERS_RT_PROJECT_DIRECTORY="/../whitelabel/consumers-rt/" \
PRODUCERS_PROJECT_DIRECTORY="/../whitelabel/producers/" \
MASTER_PROJECT_COMMAND="/root/wasp/bin/whitelabel-master" \
CONSUMERS_SPARK_PROJECT_COMMAND="/root/wasp/bin/whitelabel-consumers-spark" \
CONSUMERS_RT_PROJECT_COMMAND="/root/wasp/bin/whitelabel-consumers-rt" \
PRODUCERS_PROJECT_COMMAND="/root/wasp/bin/whitelabel-producers" \
./start-wasp.sh $@