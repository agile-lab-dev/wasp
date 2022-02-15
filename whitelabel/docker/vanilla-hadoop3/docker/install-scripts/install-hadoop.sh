#!/usr/bin/env bash

source ./common/bash-defaults.sh

wget --progress=bar:force:noscroll -O hadoop.tar.gz http://www-eu.apache.org/dist/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz

mkdir -p $HADOOP_HOME 

tar -xvf hadoop.tar.gz \
    --directory=$HADOOP_HOME\
    --strip 1 \
    --exclude=hadoop-3.2.2/share/doc

rm hadoop.tar.gz