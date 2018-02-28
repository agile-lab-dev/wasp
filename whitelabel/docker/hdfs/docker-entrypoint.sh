#!/bin/bash
#
# docker-entrypoint for docker-solr

set -e

if [[ "$VERBOSE" = "yes" ]]; then
    set -x
fi

# when invoked with e.g.: docker run solr -help
if [ ! -f /data/dfs/namenode/current/VERSION ]; then
    echo "$2"
    if [[ "$2" = "namenode" ]]; then
        mkdir -p /data/dfs/namenode /data/dfs/checkpoint
        hdfs namenode -format  -force -nonInteractive -clusterid CID-8bf63244-0510-4db6-a949-8f74b50f2be9
    fi
fi

# execute command passed in as arguments.
# The Dockerfile has specified the PATH to include
# /opt/solr/bin (for Solr) and /opt/docker-solr/scripts (for our scripts
# like solr-foreground, solr-create, solr-precreate, solr-demo).
# Note: if you specify "solr", you'll typically want to add -f to run it in
# the foreground.
exec "$@"
