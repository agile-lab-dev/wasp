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

cd $SCRIPT_DIR/../../..

echo "Running sbt stage task..."
sbt -mem 4096 wasp-whitelabel/stage

TAG_NAME=$1
IMAGE_NAME=registry.gitlab.com/agilefactory/agile.wasp2/try-wasp:$1

echo "building $IMAGE_NAME"

tar cf - \
       -C `realpath $SCRIPT_DIR/../../` master/target/universal/stage \
       -C `realpath $SCRIPT_DIR/../../` consumers-spark/target/universal/stage \
       -C `realpath $SCRIPT_DIR/../../` producers/target/universal/stage \
       -C $SCRIPT_DIR docker-environment.conf \
       -C $SCRIPT_DIR log4j2.properties \
       -C $SCRIPT_DIR docker-entrypoint.sh \
       -C $SCRIPT_DIR supervisord.conf \
       -C $SCRIPT_DIR templates/hbase-site.xml \
       -C $SCRIPT_DIR Dockerfile | docker build -t $IMAGE_NAME -f Dockerfile -

echo "built $IMAGE_NAME"
echo "use docker push $IMAGE_NAME to push it to the registry"
