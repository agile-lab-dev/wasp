#!/usr/bin/env bash

# absolute path to this script. /home/user/bin/foo.sh
SCRIPT=$(readlink -f $0)
# absolute path this script is in. /home/user/bin
SCRIPT_DIR=`dirname $SCRIPT`
# cd into the project root
cd ${SCRIPT_DIR}/..

# define colors
red=$(tput setaf 1)
green=$(tput setaf 2)
yellow=$(tput setaf 3)
blue=$(tput setaf 4)
default=$(tput sgr0)

# kill everything on exit
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

# run stage task
sbt stage

# launch each module
LOG4J2_OPTS="-Dlog4j.configurationFile=$SCRIPT_DIR/log4j2.properties"
master/target/universal/stage/bin/wasp-master ${LOG4J2_OPTS} 2>&1 | sed "s/.*/$red&$default/" &
producers/target/universal/stage/bin/wasp-producers ${LOG4J2_OPTS} 2>&1 | sed "s/.*/$green&$default/" &
consumers-rt/target/universal/stage/bin/wasp-consumers-rt ${LOG4J2_OPTS} 2>&1 | sed "s/.*/$yellow&$default/" &
consumers-spark/target/universal/stage/bin/wasp-consumers-spark ${LOG4J2_OPTS} 2>&1 | sed "s/.*/$blue&$default/" &

# wait for all children to end
wait