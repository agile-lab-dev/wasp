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

# package everything
sbt package

# launch each module
sbt wasp-master/run 2>&1 | sed "s/.*/$red&$default/" &
sbt wasp-producers/run 2>&1 | sed "s/.*/$green&$default/" &
sbt wasp-consumers-rt/run 2>&1 | sed "s/.*/$yellow&$default/" &
sbt wasp-consumers-spark/run 2>&1 | sed "s/.*/$blue&$default/" &

# wait for all children to end
wait