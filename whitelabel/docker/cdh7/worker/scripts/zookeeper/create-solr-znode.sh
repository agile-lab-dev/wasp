#!/bin/bash
set -eax
source /opt/scripts/retry.sh
retry 5 zookeeper-client create /solr