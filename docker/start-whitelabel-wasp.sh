#!/usr/bin/env bash

MASTER_PROJECT_DIRECTORY="/../whitelabel/master/" \
PRODUCERS_PROJECT_DIRECTORY="/../whitelabel/producers/" \
CONSUMERS_SPARK_PROJECT_DIRECTORY="/../whitelabel/consumers-spark/" \
CONSUMERS_RT_PROJECT_DIRECTORY="/../whitelabel/consumers-rt/" \
MASTER_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-master" \
PRODUCERS_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-producers" \
CONSUMERS_SPARK_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-consumers-spark" \
CONSUMERS_RT_PROJECT_COMMAND="/root/wasp/bin/wasp-whitelabel-consumers-rt" \
./start-wasp.sh $@