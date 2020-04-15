#!/bin/bash

set -aex

mkdir -p /var/log/hadoop-yarn/

yarn resourcemanager &> /var/log/hadoop-yarn/resourcemanager.log &
yarn nodemanager &> /var/log/hadoop-yarn/nodemanager.log &