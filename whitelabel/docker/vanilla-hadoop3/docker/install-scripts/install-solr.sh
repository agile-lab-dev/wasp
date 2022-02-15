#!/usr/bin/env bash
source ./common/bash-defaults.sh

# Download and unpackage solr 8
SOLR_VERSION=8.5.1
SOLR_PKG_NAME=solr-$SOLR_VERSION

wget --progress=bar:force:noscroll --no-check-certificate https://archive.apache.org/dist/lucene/solr/$SOLR_VERSION/$SOLR_PKG_NAME.tgz
tar -xzvf $SOLR_PKG_NAME.tgz
rm $SOLR_PKG_NAME.tgz
mv $SOLR_PKG_NAME $SOLR_HOME

cp $SOLR_HOME/server/solr/solr.xml $SOLR_HOME/