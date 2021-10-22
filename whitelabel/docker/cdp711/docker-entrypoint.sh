#!/usr/bin/env bash
set -eax

$SOLR_HOME/bin/solr zk upconfig  -n schemalessTemplate -d $SOLR_HOME/schemalessTemplate -z $HOSTNAME:2181/solr

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf