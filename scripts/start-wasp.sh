#!/usr/bin/env bash

# cd into the project root
SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR/..

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
master/target/universal/stage/bin/wasp-master 2>&1 | sed "s/.*/$red&$default/" &
producers/target/universal/stage/bin/wasp-producers 2>&1 | sed "s/.*/$green&$default/" &
consumers-rt/target/universal/stage/bin/wasp-consumers-rt 2>&1 | sed "s/.*/$yellow&$default/" &
consumers-spark/target/universal/stage/bin/wasp-consumers-spark 2>&1 | sed "s/.*/$blue&$default/" &

# wait for all children to end
wait