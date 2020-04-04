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

# !!! standalone applications have to remove the following default settings !!! #
SBT_STAGE_COMMAND_PROJECTID="${SBT_STAGE_COMMAND_PROJECTID:-"wasp"}"
MASTER_PROJECT_DIRECTORY="$SCRIPT_DIR${MASTER_PROJECT_DIRECTORY:-"/../../master/"}"
PRODUCERS_PROJECT_DIRECTORY="$SCRIPT_DIR${PRODUCERS_PROJECT_DIRECTORY:-"/../../producers/"}"
CONSUMERS_RT_PROJECT_DIRECTORY="$SCRIPT_DIR${CONSUMERS_RT_PROJECT_DIRECTORY:-"/../../consumers-rt/"}"
CONSUMERS_SPARK_PROJECT_DIRECTORY="$SCRIPT_DIR${CONSUMERS_SPARK_PROJECT_DIRECTORY:-"/../../consumers-spark/"}"
MASTER_PROJECT_COMMAND=${MASTER_PROJECT_COMMAND:-"/root/wasp/bin/wasp-master"}
PRODUCERS_PROJECT_COMMAND=${PRODUCERS_PROJECT_COMMAND:-"/root/wasp/bin/wasp-producers"}
CONSUMERS_RT_PROJECT_COMMAND=${CONSUMERS_RT_PROJECT_COMMAND:-"/root/wasp/bin/wasp-consumers-rt"}
CONSUMERS_SPARK_PROJECT_COMMAND=${CONSUMERS_SPARK_PROJECT_COMMAND:-"/root/wasp/bin/wasp-consumers-spark"}
CONSUMERS_SPARK_STREAMING_MAIN_CLASS=${CONSUMERS_SPARK_STREAMING_MAIN_CLASS:-"it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersStreamingNodeLauncher"}
CONSUMERS_SPARK_BATCH_MAIN_CLASS=${CONSUMERS_SPARK_BATCH_MAIN_CLASS:-"it.agilelab.bigdata.wasp.consumers.spark.launcher.SparkConsumersBatchNodeLauncher"}

# parse command line arguments
DROP_MONGODB_OPT=""
WASP_LAUNCHER_OPTS="--"
WASP_SECURITY=false
WASP_YARN=false
WASP_CONFIGURATION_FILE="docker-environment.conf"
while [[ $# -gt 0 ]]; do
    ARG=$1
    case $ARG in
        -d|--drop-db)
            DROP_MONGODB_OPT="$ARG"
            shift # past argument
        ;;
        -s|--security)
            WASP_SECURITY=true
            WASP_YARN=true
            shift # past argument
        ;;
        -y|--yarn)
            WASP_YARN=true
            shift # past argument
        ;;
        -c|--config)
            shift # past argument
            WASP_CONFIGURATION_FILE=$(echo -e "${1}" | sed -e 's/^[[:space:]]*//')
            shift # past argument
        ;;
        *)
            WASP_LAUNCHER_OPTS+=" $ARG"
            shift # past argument
        ;;
    esac
done

#echo "Options:"
#echo "DROP_MONGODB_OPT: '$DROP_MONGODB_OPT'"
#echo "WASP_LAUNCHER_OPTS: '$WASP_LAUNCHER_OPTS'"
#echo "WASP_SECURITY: '$WASP_SECURITY'"
#echo "WASP_YARN: '$WASP_YARN'"
#echo -e "WASP_CONFIGURATION_FILE: '$WASP_CONFIGURATION_FILE'\n"

# prepare binaries for running
cd $SCRIPT_DIR/../..
# cd $SCRIPT_DIR/.. # !!! standalone applications have to use this !!! #

echo "Running sbt stage task..."
sbt -mem 3072 ${SBT_STAGE_COMMAND_PROJECTID}/stage
#sbt -v -mem 3072 ${SBT_STAGE_COMMAND_PROJECTID}/stage

# get docker command, init network if needed
cd $SCRIPT_DIR
source get-docker-cmd.sh
source create-wasp-network.sh

# define colors
r=$(tput setaf 1)
g=$(tput setaf 2)
y=$(tput setaf 3)
b=$(tput setaf 4)
p=$(tput setaf 5)
a=$(tput setaf 6)
d=$(tput sgr0)

# launch each module
WASP_OPTS="-J-Xmx1g -J-Xms512m -Dlog4j.configurationFile=/root/configurations/log4j2.properties -Dconfig.file=/root/configurations/${WASP_CONFIGURATION_FILE} "
WASP_OPTS="$WASP_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -J-XX:+HeapDumpOnOutOfMemoryError -J-XX:HeapDumpPath=/root/"
DOCKER_OPTS="-i -v $SCRIPT_DIR:/root/configurations/:ro --network=wasp-docker --rm -w /root/configurations/"
DOCKER_IMAGE="agilefactory/oracle-java:jdk-8u181"

SECURITY_DOCKER_OPTS=""
if [ "$WASP_SECURITY" = true ]; then
    echo -e "\nEnabling security..."
    source security-env.sh
    WASP_OPTS="$WASP_OPTS -Djava.security.krb5.conf=/root/configurations/krb5.conf -Djava.security.auth.login.config=/root/configurations/sasl.jaas.config -Djava.net.preferIPv4Stack=true -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true"
    SECURITY_DOCKER_OPTS="-e PRINCIPAL_NAME=$PRINCIPAL_NAME -e WASP_SECURITY=$WASP_SECURITY -e KEYTAB_FILE_NAME=$KEYTAB_FILE_NAME -e KRB5_CONFIG=/root/configurations/krb5.conf  $ETC_HOSTS -e HADOOP_JAAS_DEBUG=true"
fi

if [ "$WASP_YARN" = true ]; then
    echo -e "\nSetting YARN-mode docker options..."
    DOCKER_OPTS="$DOCKER_OPTS -v $SCRIPT_DIR/external-cluster-configuration/hadoop:/etc/hadoop/conf/:ro"
else
    echo -e "\nSetting NOT YARN-mode docker options..."
    DOCKER_OPTS="$DOCKER_OPTS -v $SCRIPT_DIR/docker-service-configuration/hadoop:/etc/hadoop/conf/:ro"
fi

if [ -n "$DROP_MONGODB_OPT" ]; then
    echo -e "\nRunning module 'master' module with 'dropDB option' in container in order to drop MongoDB database..."

    $DOCKER_CMD run ${DOCKER_OPTS} --name master \
    -p 2891:2891 -p 5005:5005 \
    ${SECURITY_DOCKER_OPTS} \
    -v "$MASTER_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/:ro \
    ${DOCKER_IMAGE} /root/configurations/entrypoint.sh ${MASTER_PROJECT_COMMAND} ${WASP_OPTS} \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
    -Dwasp.akka.remote.netty.tcp.hostname="master" \
    ${WASP_LAUNCHER_OPTS} ${DROP_MONGODB_OPT} 2>&1  | sed "s/.*/$a master-dropMongoDB           |$d &/" &

    wait
fi

echo -e "\nRunning modules in containers..."
#echo "DOCKER_OPTS: '$DOCKER_OPTS'"
#echo "SECURITY_DOCKER_OPTS: '$SECURITY_DOCKER_OPTS'"
#echo "WASP_OPTS: '$WASP_OPTS'"
#echo -e "WASP_LAUNCHER_OPTS: '$WASP_LAUNCHER_OPTS'\n"

$DOCKER_CMD run ${DOCKER_OPTS} --name master \
    -p 2891:2891 -p 5005:5005 \
    ${SECURITY_DOCKER_OPTS} \
    -v "$MASTER_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/:ro \
    ${DOCKER_IMAGE} /root/configurations/entrypoint.sh ${MASTER_PROJECT_COMMAND} ${WASP_OPTS} \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
    -Dwasp.akka.remote.netty.tcp.hostname="master" \
    ${WASP_LAUNCHER_OPTS} 2>&1                      | sed "s/.*/$r master                       |$d &/" &

$DOCKER_CMD run ${DOCKER_OPTS} --name producers \
    -p 5006:5006 \
    ${SECURITY_DOCKER_OPTS} \
    -v "$PRODUCERS_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/:ro \
    ${DOCKER_IMAGE} /root/configurations/entrypoint.sh ${PRODUCERS_PROJECT_COMMAND} ${WASP_OPTS} \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 \
    -Dwasp.akka.remote.netty.tcp.hostname="producers" \
    ${WASP_LAUNCHER_OPTS} 2>&1                      | sed "s/.*/$g producers                    |$d &/" &

$DOCKER_CMD run ${DOCKER_OPTS} --name consumers-rt \
    -p 5007:5007 \
    ${SECURITY_DOCKER_OPTS} \
    -v "$CONSUMERS_RT_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/:ro \
    ${DOCKER_IMAGE} /root/configurations/entrypoint.sh ${CONSUMERS_RT_PROJECT_COMMAND} ${WASP_OPTS} \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007 \
    -Dwasp.akka.remote.netty.tcp.hostname="consumers-rt" \
    ${WASP_LAUNCHER_OPTS} 2>&1                      | sed "s/.*/$y consumers-rt                 |$d &/" &

$DOCKER_CMD run ${DOCKER_OPTS} --name consumers-spark-streaming \
    -p 4040:4040 -p 5008:5008 -p 9010:9010 -p 31541:31541 -p 31542:31542 \
    ${SECURITY_DOCKER_OPTS} \
    -e HADOOP_CONF_DIR=/etc/hadoop/conf/ \
    -v "$CONSUMERS_SPARK_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/:ro \
    ${DOCKER_IMAGE} /root/configurations/entrypoint.sh ${CONSUMERS_SPARK_PROJECT_COMMAND} \
    -main ${CONSUMERS_SPARK_STREAMING_MAIN_CLASS} ${WASP_OPTS} \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5008 \
    -Dwasp.akka.remote.netty.tcp.hostname="consumers-spark-streaming" \
    -Dwasp.akka.cluster.roles.0="consumers-spark-streaming" \
    ${WASP_LAUNCHER_OPTS} 2>&1                      | sed "s/.*/$b consumers-spark-streaming    |$d &/" &

$DOCKER_CMD run ${DOCKER_OPTS} --name consumers-spark-batch \
    -p 4039:4040 -p 5009:5009 -p 31441:31441 -p 31442:31442 \
    ${SECURITY_DOCKER_OPTS} \
    -e HADOOP_CONF_DIR=/etc/hadoop/conf/ \
    -v "$CONSUMERS_SPARK_PROJECT_DIRECTORY/target/universal/stage/":/root/wasp/:ro \
    ${DOCKER_IMAGE} /root/configurations/entrypoint.sh ${CONSUMERS_SPARK_PROJECT_COMMAND} \
    -main ${CONSUMERS_SPARK_BATCH_MAIN_CLASS} ${WASP_OPTS} \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009 \
    -Dwasp.akka.remote.netty.tcp.hostname="consumers-spark-batch" \
    -Dwasp.akka.cluster.roles.0="consumers-spark-batch" \
    ${WASP_LAUNCHER_OPTS} 2>&1                      | sed "s/.*/$p consumers-spark-batch        |$d &/" &

# wait for all children to end or SIGINT/SIGTERM
wait
