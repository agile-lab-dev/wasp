#!/usr/bin/env bash

# exit on any error
set -e

# absolute path to this script. /home/user/bin/foo.sh
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
# this variable contains the directory of the script
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

source $SCRIPT_DIR/../get-docker-cmd.sh

cd $SCRIPT_DIR

${DOCKER_CMD} build -t agilefactory/oracle-java:jdk-8u162 .
${DOCKER_CMD} tag agilefactory/oracle-java:jdk-8u162 agilefactory/oracle-java:jdk-8