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
export SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo $SCRIPT_DIR
SBT_STAGE_COMMAND_PROJECTID="wasp-whitelabel"
export MASTER_PROJECT_DIRECTORY="$SCRIPT_DIR/../../../whitelabel/master/"
export PRODUCERS_PROJECT_DIRECTORY="$SCRIPT_DIR/../../../whitelabel/producers/"
export CONSUMERS_SPARK_PROJECT_DIRECTORY="$SCRIPT_DIR/../../../whitelabel/consumers-spark/"
export CONSUMERS_RT_PROJECT_DIRECTORY="$SCRIPT_DIR/../../../whitelabel/consumers-spark/"

# parse command line arguments
export DROP_WASPDB=false
export WASP_CONFIGURATION_FILE="docker-environment.conf"
export DELETE_DOCKER=true
export CLEAN=false
SKIP_BUILD=false
CLEAN=false

while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        -c|--config)
            shift # past argument
            export WASP_CONFIGURATION_FILE=$(echo -e "${1}" | sed -e 's/^[[:space:]]*//')
            shift # past argument
        ;;
       --clean)
            CLEAN=true
            shift
        ;;
       -d)
            export DROP_WASPDB=true
            shift
        ;;
       --persist)
            export DELETE_DOCKER=false
            shift # past argument
        ;;
        --clean)
            CLEAN=true
            shift
        ;;
        --skip-build)
            SKIP_BUILD=true
            shift
        ;;
        *)
            export WASP_LAUNCHER_OPTS+=" $ARG"
            shift # past argument
        ;;
    esac
done

if $SKIP_BUILD; then
  echo "skipping build..."
else
  echo "Running sbt stage task..."
  export WASP_FLAVOR=VANILLA2_2_12
  if $CLEAN; then
    SBT_CMD="sbt -mem 8000 clean ${SBT_STAGE_COMMAND_PROJECTID}/stage"
  else
    SBT_CMD="sbt -mem 3072 ${SBT_STAGE_COMMAND_PROJECTID}/stage"
  fi
  # prepare binaries for running
  echo $SBT_CMD
  (cd $SCRIPT_DIR/../../../; $SBT_CMD)
fi

# get docker command, init network if needed
source $SCRIPT_DIR/get-docker-cmd.sh

export DOCKER_IMAGE='registry.gitlab.com/agilefactory/agile.wasp2/wasp-hadoop-vanilla-2.12:2'
export NAME_CONTAINER=wasp-whitelabel

set -ax

if $DELETE_DOCKER; then
  docker rm $NAME_CONTAINER || true
fi

$DOCKER_CMD run -it --name $NAME_CONTAINER \
  -v $MASTER_PROJECT_DIRECTORY/target/universal/stage/:/code/master \
  -v $PRODUCERS_PROJECT_DIRECTORY/target/universal/stage/:/code/producer \
  -v $CONSUMERS_RT_PROJECT_DIRECTORY/target/universal/stage/:/code/consumers-rt \
  -v $CONSUMERS_SPARK_PROJECT_DIRECTORY/target/universal/stage/:/code/consumers-spark \
  -v $SCRIPT_DIR/$WASP_CONFIGURATION_FILE:/wasp.conf \
  -v $SCRIPT_DIR/log4j-consumer.properties:/log4j-consumer.properties \
  -v $SCRIPT_DIR/log4j-master.properties:/log4j-master.properties \
  -v $SCRIPT_DIR/log4j-producer.properties:/log4j-producer.properties \
  -v $SCRIPT_DIR/docker-entrypoint.sh:/docker-entrypoint.sh \
  -v $SCRIPT_DIR/supervisord.conf:/etc/supervisor/conf.d/supervisord.conf \
  -p 7180:7180 \
  -p 2222:22 \
  -p 60010:60010 \
  -p 60020:60020 \
  -p 8088:8088 \
  -p 8042:8042 \
  -p 8983:8983 \
  -p 27017:27017 \
  -p 5005:5005 \
  -p 5006:5006 \
  -p 5007:5007 \
  -p 5008:5008 \
  -p 5009:5009 \
  -p 20000:20000 \
  -p 2891:2891 \
  -p 18080:18080 \
  -p 8080:8080 \
  --entrypoint /docker-entrypoint.sh \
  $DOCKER_IMAGE


#  -v $SCRIPT_DIR/templates/hbase-site.xml:/templates/hbase-site.xml \
