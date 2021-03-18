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

SBT_STAGE_COMMAND_PROJECTID="wasp-whitelabel"
MASTER_PROJECT_DIRECTORY="$SCRIPT_DIR/../../master/"
PRODUCERS_PROJECT_DIRECTORY="$SCRIPT_DIR/../../producers/"
CONSUMERS_RT_PROJECT_DIRECTORY="$SCRIPT_DIR/../../consumers-rt/"
CONSUMERS_SPARK_PROJECT_DIRECTORY="$SCRIPT_DIR/../../consumers-spark/"
CONSUMERS_SPARK_STREAMING_MAIN_CLASS=""
CONSUMERS_SPARK_BATCH_MAIN_CLASS=""

# parse command line arguments
DROP_MONGODB_OPT=""
WASP_LAUNCHER_OPTS="--"
WASP_SECURITY=false
WASP_YARN=false
WASP_CONFIGURATION_FILE="docker-environment.conf"
PERSIST_FLAG=false
while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        -d|--drop-db)
            DROP_MONGODB_OPT="$ARG"
            shift # past argument
        ;;
        -c|--config)
            shift # past argument
            WASP_CONFIGURATION_FILE=$(echo -e "${1}" | sed -e 's/^[[:space:]]*//')
            shift # past argument
        ;;
       -p|--persist)
            PERSIST_FLAG=true
            shift # past argument
        ;;
        *)
            WASP_LAUNCHER_OPTS+=" $ARG"
            shift # past argument
        ;;
    esac
done
# prepare binaries for running
cd $SCRIPT_DIR/../../..

echo "Running sbt stage task..."
sbt -mem 3072 ${SBT_STAGE_COMMAND_PROJECTID}/stage
#sbt -v -mem 3072 ${SBT_STAGE_COMMAND_PROJECTID}/stage

# get docker command, init network if needed
cd $SCRIPT_DIR
source get-docker-cmd.sh

DOCKER_IMAGE=registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:6.3.2-nifi-confluent
NAME_CONTAINER=agile-wasp-2-whitelabel

set -ax

if [ "$PERSIST_FLAG" = false ]
 then
  docker rm $NAME_CONTAINER || true
  $DOCKER_CMD run -it --name $NAME_CONTAINER --network wasp2 \
    -v $MASTER_PROJECT_DIRECTORY/target/universal/stage/:/code/master \
    -v $PRODUCERS_PROJECT_DIRECTORY/target/universal/stage/:/code/producer \
    -v $CONSUMERS_RT_PROJECT_DIRECTORY/target/universal/stage/:/code/consumers-rt \
    -v $CONSUMERS_SPARK_PROJECT_DIRECTORY/target/universal/stage/:/code/consumers-spark \
    -v $SCRIPT_DIR/$WASP_CONFIGURATION_FILE:/wasp.conf \
    -v $SCRIPT_DIR/log4j2.properties:/log4j2.properties \
    -v $SCRIPT_DIR/docker-entrypoint.sh:/docker-entrypoint.sh \
    -v $SCRIPT_DIR/supervisord.conf:/etc/supervisor/conf.d/supervisord.conf \
    -v $SCRIPT_DIR/templates/hbase-site.xml:/templates/hbase-site.xml \
    -v $SCRIPT_DIR/server.py:/server.py \
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
    -p 9092:9092 \
    -p 9000:9000 \
    $DOCKER_IMAGE \
    bash /docker-entrypoint.sh

else
  $DOCKER_CMD start $NAME_CONTAINER -i
fi
