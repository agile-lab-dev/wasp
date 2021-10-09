#!/usr/bin/env bash

source ./common/bash-defaults.sh

wget --progress=bar:force:noscroll -O hadoop.tar.gz https://downloads.apache.org/hadoop/common/stable2/hadoop-2.10.1.tar.gz

mkdir -p $HADOOP_HOME 

tar -xvf hadoop.tar.gz \
    --directory=$HADOOP_HOME\
    --strip 1 \
    --exclude=hadoop-2.10.1/share/doc

rm hadoop.tar.gz