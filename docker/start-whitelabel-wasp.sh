#!/usr/bin/env bash

MASTER_PROJECT_DIRECTORY="/../white-label/master/" \
CONSUMERS_SPARK_PROJECT_DIRECTORY="/../white-label/consumers-spark/" \
CONSUMERS_RT_PROJECT_DIRECTORY="/../white-label/consumers-rt/" \
PRODUCERS_PROJECT_DIRECTORY="/../white-label/producers/" \
MASTER_PROJECT_COMMAND="/root/wasp/bin/white-label-master" \
CONSUMERS_SPARK_PROJECT_COMMAND="/root/wasp/bin/white-label-consumers-spark" \
CONSUMERS_RT_PROJECT_COMMAND="/root/wasp/bin/white-label-consumers-rt" \
PRODUCERS_PROJECT_COMMAND="/root/wasp/bin/white-label-producers" \
./start-wasp.sh $@