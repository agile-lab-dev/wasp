#!/usr/bin/env bash
source ./common/bash-defaults.sh

# Download and unpackage hbase 2.1.10
HBASE_VERSION=2.1.10
HBASE_PKG_NAME=hbase-$HBASE_VERSION-bin
HBASE_CONTENT_NAME=hbase-$HBASE_VERSION

wget --progress=bar:force:noscroll \
     --no-check-certificate https://archive.apache.org/dist/hbase/$HBASE_VERSION/$HBASE_PKG_NAME.tar.gz

mkdir $HBASE_HOME

tar -xvf $HBASE_PKG_NAME.tar.gz \
    --directory=$HBASE_HOME \
    --strip 1 \
    --exclude=$HBASE_CONTENT_NAME/docs

rm $HBASE_PKG_NAME.tar.gz

cp $HBASE_HOME/lib/client-facing-thirdparty/*.jar $HBASE_HOME/lib/