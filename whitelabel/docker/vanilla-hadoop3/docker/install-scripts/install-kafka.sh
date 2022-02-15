#!/usr/bin/env bash
source ./common/bash-defaults.sh

# Download and unpackage kafka 2.6.3
KAFKA_VERSION=2.8.1
KAFKA_PKG_NAME=kafka_2.12-$KAFKA_VERSION
wget --progress=bar:force:noscroll \
     --no-check-certificate https://downloads.apache.org/kafka/$KAFKA_VERSION/$KAFKA_PKG_NAME.tgz
tar -xzvf $KAFKA_PKG_NAME.tgz
rm $KAFKA_PKG_NAME.tgz
mv $KAFKA_PKG_NAME $KAFKA_HOME