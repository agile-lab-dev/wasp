#!/usr/bin/env bash

source ./common/bash-defaults.sh

echo "Configuring spark classpath"

hadoop classpath --glob | sed 's/:/\n/g' | grep 'jar' | grep -v netty | grep -v jackson | sed 's|^|cp |g' | sed 's|$| $SPARK_HOME/jars|g' | bash