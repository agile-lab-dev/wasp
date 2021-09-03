#!/usr/bin/env bash
source ./common/bash-defaults.sh

# Download and unpackage spark 2.4.7
SPARK_VERSION=2.4.7
SPARK_PKG_NAME=spark-$SPARK_VERSION-bin-without-hadoop
wget --progress=bar:force:noscroll \
     --no-check-certificate https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK_PKG_NAME.tgz

tar -xzvf $SPARK_PKG_NAME.tgz

rm $SPARK_PKG_NAME.tgz
mv $SPARK_PKG_NAME $SPARK_HOME