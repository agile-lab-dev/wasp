#!/usr/bin/env bash

echo "Finding whether docker-compose can be run without sudo"

# attempt to run an innocuous docker-compose command
set +e
docker-compose -f $SCRIPT_DIR/mongodb-docker-compose.yml ps &> /dev/null
EXIT_CODE=$?
set -e

# check exit code; if 1, assume permission error
if [ $EXIT_CODE -eq 0 ]; then
    echo "Command \"docker-compose ps\" succeeded, using \"docker-compose\" as command"
    DOCKER_COMPOSE_CMD="docker-compose"
else
    echo "Command \"docker-compose ps\" failed, using \"sudo docker-compose\" as command"
    DOCKER_COMPOSE_CMD="sudo docker-compose"
fi