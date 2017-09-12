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
# This variable contains the directory of the script
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $SCRIPT_DIR/..

# define colors
r=$(tput setaf 1) # red
g=$(tput setaf 2) # green
y=$(tput setaf 3) # yellow
b=$(tput setaf 4) # blue
d=$(tput sgr0) # default

# kill everything on exit
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

# run stage task
sbt stage

# launch each module
LOG4J2_OPTS="-Dlog4j.configurationFile=$SCRIPT_DIR/log4j2.properties"
master/target/universal/stage/bin/wasp-master ${LOG4J2_OPTS} 2>&1                   | sed "s/.*/$r master           |$d &/" &
producers/target/universal/stage/bin/wasp-producers ${LOG4J2_OPTS} 2>&1             | sed "s/.*/$g producers        |$d &/" &
consumers-rt/target/universal/stage/bin/wasp-consumers-rt ${LOG4J2_OPTS} 2>&1       | sed "s/.*/$y consumers-rt     |$d &/" &
consumers-spark/target/universal/stage/bin/wasp-consumers-spark ${LOG4J2_OPTS} 2>&1 | sed "s/.*/$b consumers-spark  |$d &/" &

# wait for all children to end
wait