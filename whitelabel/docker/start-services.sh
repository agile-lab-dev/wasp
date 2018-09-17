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

# help
HELP="Usage: start-services.sh [options] SERVICE...

    Options:
    -h, --help                  show this help and exit

    Services:
    elasticsearch               start elasticsearch (and kibana)
    hbase                       start hbase (and hdfs)
    hdfs                        start hdfs
    kafka                       start kafka (and zookeeper)
    mysql                       start mysql
    solr                        start solr (and banana)
    spark                       start spark

    The MongoDB service is always started.
"

# parse command line arguments
ELASTICSEARCH=0
HBASE=0
HDFS=0
KAFKA=0
MYSQL=0
SOLR=0
SPARK=0
while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        -h|--help)
            echo "$HELP"
            exit 0
        ;;
        "elasticsearch")
            ELASTICSEARCH=1
            shift # past argument
        ;;
        "hbase")
            HBASE=1
            HDFS=1
            shift # past argument
        ;;
        "hdfs")
            HDFS=1
            shift # past argument
        ;;
        "kafka")
            KAFKA=1
            shift # past argument
        ;;
        "mysql")
            MYSQL=1
            shift # past argument
        ;;
        "solr")
            SOLR=1
            shift # past argument
        ;;
        "spark")
            SPARK=1
            shift # past argument
        ;;
        *)
            echo "Unknown option/service $ARG"
            echo "$HELP"
            exit 1
        ;;
    esac
done

# decide which compose files to use
COMPOSE_FILES=("mongodb-docker-compose.yml")

if [ ${ELASTICSEARCH} -eq 1 ]; then
    COMPOSE_FILES+=("elastickibana-docker-compose.yml")
fi
if [ ${HBASE} -eq 1 ]; then
    COMPOSE_FILES+=("hbase-docker-compose.yml")
fi
if [ ${HDFS} -eq 1 ]; then
    COMPOSE_FILES+=("hdfs-docker-compose.yml")
fi
if [ ${KAFKA} -eq 1 ]; then
    COMPOSE_FILES+=("kafka-docker-compose.yml")
fi
if [ ${MYSQL} -eq 1 ]; then
    COMPOSE_FILES+=("mysql-docker-compose.yml")
fi
if [ ${SOLR} -eq 1 ]; then
    COMPOSE_FILES+=("solrcloudbanana-docker-compose.yml")
fi
if [ ${SPARK} -eq 1 ]; then
    COMPOSE_FILES+=("spark-docker-compose.yml")
fi


# get docker command, init network if needed
cd $SCRIPT_DIR
source get-docker-compose-cmd.sh
source create-wasp-network.sh

# build string containing docker-compose compose files options
COMPOSE_FILES_OPTIONS=""
for COMPOSE_FILE in ${COMPOSE_FILES[@]}; do
    COMPOSE_FILES_OPTIONS="$COMPOSE_FILES_OPTIONS -f $COMPOSE_FILE"
done

# run compose files
$DOCKER_COMPOSE_CMD $COMPOSE_FILES_OPTIONS up