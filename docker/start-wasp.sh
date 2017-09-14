#!/usr/bin/env bash

# absolute path to this script. /home/user/bin/foo.sh
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
# This variable contains the directory of the script
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $SCRIPT_DIR/..

# define colors
r=$(tput setaf 1)
g=$(tput setaf 2)
y=$(tput setaf 3)
b=$(tput setaf 4)
d=$(tput sgr0)

set -e
echo "run stage task"
sbt stage
set +e

docker network create  wasp-docker

# launch each module
WASP_OPTS="-J-Xmx1g -J-Xms512m -Dlog4j.configurationFile=/root/configurations/log4j2.properties -Dconfig.file=/root/configurations/docker-environment.conf"
DOCKER_LOG4J2_OPTS="-v $SCRIPT_DIR:/root/configurations"
DOCKER_OPTS="-it --network=wasp-docker --rm "
DOCKER_IMAGE="sgrio/java-oracle:jre_8 "
echo "Build master components Image"
docker run  ${DOCKER_LOG4J2_OPTS} ${DOCKER_OPTS} --name master -p 8082:8080 -v "$SCRIPT_DIR/../master/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE}  /root/wasp/bin/wasp-master ${WASP_OPTS} -Dwasp.akka.remote.netty.tcp.hostname="master" 2>&1                         | sed "s/.*/$r master           |$d &/" &
docker run  ${DOCKER_LOG4J2_OPTS} ${DOCKER_OPTS} --name producers -v "$SCRIPT_DIR/../producers/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE}  /root/wasp/bin/wasp-producers ${WASP_OPTS} -Dwasp.akka.remote.netty.tcp.hostname="producers" 2>&1                          | sed "s/.*/$g producers        |$d &/" &
docker run  ${DOCKER_LOG4J2_OPTS} ${DOCKER_OPTS} --name consumers-rt -v "$SCRIPT_DIR/../consumers-rt/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE}  /root/wasp/bin/wasp-consumers-rt ${WASP_OPTS} -Dwasp.akka.remote.netty.tcp.hostname="consumers-rt" 2>&1              | sed "s/.*/$y consumers-rt     |$d &/" &
docker run  ${DOCKER_LOG4J2_OPTS} ${DOCKER_OPTS} --name consumers-spark -v "$SCRIPT_DIR/../consumers-spark/target/universal/stage/":/root/wasp/ ${DOCKER_IMAGE}  /root/wasp/bin/wasp-consumers-spark ${WASP_OPTS} -Dwasp.akka.remote.netty.tcp.hostname="consumers-spark" 2>&1  | sed "s/.*/$b consumers-spark  |$d &/" &

# wait for all children to end
wait