#!/bin/bash
#
# docker-entrypoint for docker-solr

set -e

if [[ "$VERBOSE" = "yes" ]]; then
    set -x
fi

# when invoked with e.g.: docker run solr -help
if [ -f /tmp/namenodeformat ]; then
    echo "$2"
    if [[ "$2" = "namenode" ]]; then
        hdfs namenode -format
    fi
    rm -f /tmp/namenodeformat
fi

# execute command passed in as arguments.
# The Dockerfile has specified the PATH to include
# /opt/solr/bin (for Solr) and /opt/docker-solr/scripts (for our scripts
# like solr-foreground, solr-create, solr-precreate, solr-demo).
# Note: if you specify "solr", you'll typically want to add -f to run it in
# the foreground.
exec "$@"
