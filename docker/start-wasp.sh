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

MASTER_PROJECT_DIRECTORY="$SCRIPT_DIR/../master/"
CONSUMERS_SPARK_PROJECT_DIRECTORY="$SCRIPT_DIR/../consumers-spark/"
CONSUMERS_RT_PROJECT_DIRECTORY="$SCRIPT_DIR/../consumers-rt/"
PRODUCERS_PROJECT_DIRECTORY="$SCRIPT_DIR/../producers/"


MASTER_PROJECT_COMMAND="/root/wasp/bin/wasp-master"
CONSUMERS_SPARK_PROJECT_COMMAND="/root/wasp/bin/wasp-consumers-spark"
CONSUMERS_RT_PROJECT_COMMAND="/root/wasp/bin/wasp-consumers-rt"
PRODUCERS_PROJECT_COMMAND="/root/wasp/bin/wasp-producers"

# parse command line arguments
WASP_LAUNCHER_OPTS="--"
WASP_LAUNCHER_MASTER_OPTS=""
while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        -d|--drop-db)
            WASP_LAUNCHER_MASTER_OPTS="$WASP_LAUNCHER_MASTER_OPTS $ARG"
            shift # past argument
        ;;
        -w|--white-label)
            MASTER_PROJECT_DIRECTORY="$SCRIPT_DIR/../white-label/master/"
            CONSUMERS_SPARK_PROJECT_DIRECTORY="$SCRIPT_DIR/../white-label/consumers-spark/"
            CONSUMERS_RT_PROJECT_DIRECTORY="$SCRIPT_DIR/../white-label/consumers-rt/"
            PRODUCERS_PROJECT_DIRECTORY="$SCRIPT_DIR/../white-label/producers/"

            MASTER_PROJECT_COMMAND="/root/wasp/bin/white-label-master"
            CONSUMERS_SPARK_PROJECT_COMMAND="/root/wasp/bin/white-label-consumers-spark"
            CONSUMERS_RT_PROJECT_COMMAND="/root/wasp/bin/white-label-consumers-rt"
            PRODUCERS_PROJECT_COMMAND="/root/wasp/bin/white-label-producers"
            shift # past argument
        ;;
        *)
            WASP_LAUNCHER_OPTS="$WASP_LAUNCHER_OPTS $ARG"
            shift # past argument
        ;;
    esac
done

# prepare binaries for running
cd $SCRIPT_DIR/..
echo "Running sbt stage task..."
sbt stage

# get docker command, init network if needed
cd $SCRIPT_DIR
source get-docker-cmd.sh
source create-wasp-network.sh

# define colors
r=$(tput setaf 1)
g=$(tput setaf 2)
y=$(tput setaf 3)
b=$(tput setaf 4)
d=$(tput sgr0)

# launch each module
WASP_OPTS="-J-Xmx1g -J-Xms512m -Dlog4j.configurationFile=/root/configurations/log4j2.properties -Dconfig.file=/root/configurations/docker-environment.conf -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -J-XX:+HeapDumpOnOutOfMemoryError -J-XX:HeapDumpPath=/home/"
DOCKER_OPTS="-i -v $SCRIPT_DIR:/root/configurations --network=wasp-docker --rm "
DOCKER_IMAGE="sgrio/java-oracle:jre_8"
echo "Running modules in containers..."

echo "$DOCKER_CMD run ${DOCKER_OPTS} --name master -p 2891:2891 -p 5005:5005 -v $MASTER_PROJECT_DIRECTORY/target/universal/stage/:/root/wasp/ ${DOCKER_IMAGE} ${MASTER_PROJECT_COMMAND} ${WASP_OPTS} -jvm-debug 5005 -Dwasp.akka.remote.netty.tcp.hostname="master" ${WASP_LAUNCHER_OPTS} ${WASP_LAUNCHER_MASTER_OPTS} 2>&1"

$DOCKER_CMD run ${DOCKER_OPTS} --name master -p 2891:2891 -p 5005:5005 -v "$MASTER_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE} ${MASTER_PROJECT_COMMAND} ${WASP_OPTS} -jvm-debug 5005 -Dwasp.akka.remote.netty.tcp.hostname="master" ${WASP_LAUNCHER_OPTS} ${WASP_LAUNCHER_MASTER_OPTS} 2>&1                                                                                                                                                                | sed "s/.*/$r master           |$d &/" &
$DOCKER_CMD run ${DOCKER_OPTS} --name producers -p 5006:5006 -v "$PRODUCERS_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE} ${PRODUCERS_PROJECT_COMMAND} ${WASP_OPTS} -jvm-debug 5006 -Dwasp.akka.remote.netty.tcp.hostname="producers" ${WASP_LAUNCHER_OPTS} 2>&1                                                                                                                                                                                              | sed "s/.*/$g producers        |$d &/" &
$DOCKER_CMD run ${DOCKER_OPTS} --name consumers-rt -p 5007:5007 -v "$SCRIPT_DIR/docker-service-configuration/hdfs":/etc/hadoop/conf/ -v "$SCRIPT_DIR/docker-service-configuration/hbase":/etc/hbase/conf/ -v "$CONSUMERS_RT_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE} ${CONSUMERS_RT_PROJECT_COMMAND} ${WASP_OPTS} -jvm-debug 5007 -Dwasp.akka.remote.netty.tcp.hostname="consumers-rt" ${WASP_LAUNCHER_OPTS} 2>&1                                        | sed "s/.*/$y consumers-rt     |$d &/" &
$DOCKER_CMD run ${DOCKER_OPTS} --name consumers-spark -p 4040:4040 -p 5008:5008 -p 9010:9010 -v "$SCRIPT_DIR/docker-service-configuration/hdfs":/etc/hadoop/conf/ -v "$SCRIPT_DIR/docker-service-configuration/hbase":/etc/hbase/conf/ -v "$CONSUMERS_SPARK_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE} ${CONSUMERS_SPARK_PROJECT_COMMAND} ${WASP_OPTS} -jvm-debug 5008 -Dwasp.akka.remote.netty.tcp.hostname="consumers-spark" ${WASP_LAUNCHER_OPTS} 2>&1  | sed "s/.*/$b consumers-spark  |$d &/" &

# wait for all children to end or SIGINT/SIGTERM
wait