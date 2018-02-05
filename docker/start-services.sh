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
HELP="Usage:
    -h, --help                  show this help and exit
    -i, --indexed-datastore     specify what indexed datatore to use. Valid
                                values: \"es\", \"elastic\", \"elasticsearch\",
                                \"solr\", \"both\", \"none\"
    --no-indexed-datastore      same as \"-i none\"
    --no-spark                  do not start spark master and worker
    --no-kafka                  do not start zookeeper and kafka
    --with-hbase                start HBase
    --with-hdfs                 start HDFS
"

# parse command line arguments
INDEXED_DATASTORE=""
SPARK=1
KAFKA=1
HBASE=0
HDFS=0
while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        -h|--help)
            echo "$HELP"
            exit 0
        ;;
        -i|--indexed-datastore)
            INDEXED_DATASTORE="$2"
            shift # past argument
            shift # past value
        ;;
        --no-indexed-datastore)
            INDEXED_DATASTORE="none"
            shift # past argument
        ;;
        --no-spark)
            SPARK=0
            shift # past argument
        ;;
        --no-kafka)
            KAFKA=0
            shift # past argument
        ;;
        --with-hbase)
            HBASE=1
            shift # past argument
        ;;
        --with-hdfs)
            HDFS=1
            shift # past argument
        ;;
        *)
            echo "Unknown option $ARG"
            echo "$HELP"
            exit 1
        ;;
    esac
done

# decide which compose files to use
COMPOSE_FILES=("mongodb-docker-compose.yml")
case ${INDEXED_DATASTORE} in
    "es"|"elastic"|"elasticsearch")
        COMPOSE_FILES+=("elastickibana-docker-compose.yml")
        ;;
    "solr")
        COMPOSE_FILES+=("solrcloud-docker-compose.yml")
        ;;
    "both")
        COMPOSE_FILES+=("elastickibana-docker-compose.yml")
        COMPOSE_FILES+=("solrcloud-docker-compose.yml")
        ;;
    "none")
        ;;
    *)
        echo "No indexed datastore specified (valid: \"es\", \"elastic\", \"elasticsearch\", \"solr\", \"both\"); using elasticsearch."
        COMPOSE_FILES+=("elastickibana-docker-compose.yml")
        ;;
esac
if [ ${SPARK} -eq 1 ]; then
    COMPOSE_FILES+=("spark-docker-compose.yml")
fi
if [ ${KAFKA} -eq 1 ]; then
    COMPOSE_FILES+=("kafka-docker-compose.yml")
fi
if [ ${HBASE} -eq 1 ]; then
    COMPOSE_FILES+=("hbase-docker-compose.yml")
fi
if [ ${HDFS} -eq 1 ]; then
    COMPOSE_FILES+=("hdfs-docker-compose.yml")
fi

# get docker command, init network if needed
cd $SCRIPT_DIR
source get-docker-compose-cmd.sh
# source create-wasp-network.sh

# build string containing docker-compose compose files options
COMPOSE_FILES_OPTIONS=""
for COMPOSE_FILE in ${COMPOSE_FILES[@]}; do
    COMPOSE_FILES_OPTIONS="$COMPOSE_FILES_OPTIONS -f $COMPOSE_FILE"
done

# run compose files
$DOCKER_COMPOSE_CMD $COMPOSE_FILES_OPTIONS up
